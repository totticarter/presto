/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.operator;
import com.facebook.presto.operator.aggregation.AccumulatorFactory;
import com.facebook.presto.operator.aggregation.GroupedAccumulator;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.LongArrayBlockBuilder;
import com.facebook.presto.spi.block.VariableWidthBlockBuilder;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.facebook.presto.util.Types;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;


//import com.sun.tools.javac.util.Pair;
import com.facebook.presto.spi.type.DoubleType;

import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import com.facebook.presto.spi.block.Block;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.queryparser.ext.Extensions.Pair;

import static com.facebook.presto.operator.GroupByHash.createGroupByHash;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.nio.file.Paths;

public class HashAggregationOperator
        implements Operator
{
    public static class HashAggregationOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> groupByTypes;
        private final List<Integer> groupByChannels;
        private final Step step;
        private final List<AccumulatorFactory> accumulatorFactories;
        private final Optional<Integer> hashChannel;

        private final int expectedGroups;
        private final List<Type> types;
        private boolean closed;
        private final long maxPartialMemory;
        
        //added by cubeli
        private Map<Symbol, Integer> outputMappings;
        private Map<String, String> accumulatTypeAndColumnNameMap;
        private List<Symbol> groupByKeys;

        public HashAggregationOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> groupByTypes,
                List<Integer> groupByChannels,
                Step step,
                List<AccumulatorFactory> accumulatorFactories,
                Optional<Integer> hashChannel,
                int expectedGroups,
                DataSize maxPartialMemory,
                Map<Symbol, Integer> outputMappings,
                Map<String, String> accumulatTypeAndColumnNameMap,
                List<Symbol> groupByKeys
                )
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.hashChannel = requireNonNull(hashChannel, "hashChannel is null");
            this.groupByTypes = ImmutableList.copyOf(groupByTypes);
            this.groupByChannels = ImmutableList.copyOf(groupByChannels);
            this.step = step;
            this.accumulatorFactories = ImmutableList.copyOf(accumulatorFactories);
            this.expectedGroups = expectedGroups;
            this.maxPartialMemory = requireNonNull(maxPartialMemory, "maxPartialMemory is null").toBytes();

            this.types = toTypes(groupByTypes, step, accumulatorFactories, hashChannel);
            
            //added by cubeli
            this.outputMappings = outputMappings;
            this.accumulatTypeAndColumnNameMap = accumulatTypeAndColumnNameMap;
            this.groupByKeys = groupByKeys;
        }

        @Override
        public List<Type> getTypes()
        {
            return types;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");

            OperatorContext operatorContext;
            if (step.isOutputPartial()) {
                operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, HashAggregationOperator.class.getSimpleName(), maxPartialMemory);
            }
            else {
                operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, HashAggregationOperator.class.getSimpleName());
            }
            HashAggregationOperator hashAggregationOperator = new HashAggregationOperator(
                    operatorContext,
                    groupByTypes,
                    groupByChannels,
                    step,
                    accumulatorFactories,
                    hashChannel,
                    expectedGroups,
                    outputMappings,
                    accumulatTypeAndColumnNameMap,
                    groupByKeys);
            return hashAggregationOperator;
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new HashAggregationOperatorFactory(
                    operatorId,
                    planNodeId,
                    groupByTypes,
                    groupByChannels,
                    step,
                    accumulatorFactories,
                    hashChannel,
                    expectedGroups,
                    new DataSize(maxPartialMemory, Unit.BYTE),
                    outputMappings,
                    accumulatTypeAndColumnNameMap,
                    groupByKeys);
        }
    }

    private final OperatorContext operatorContext;
    private final List<Type> groupByTypes;
    private final List<Integer> groupByChannels;
    private final Step step;
    private final List<AccumulatorFactory> accumulatorFactories;
    private final Optional<Integer> hashChannel;
    private final int expectedGroups;

    private final List<Type> types;

    private GroupByHashAggregationBuilder aggregationBuilder;
    private Iterator<Page> outputIterator;
    private boolean finishing;
    
    //added by cubeli for lucene 
    private Map<Symbol, Integer> outputMappings;
    private Map<String, String> accumulatTypeAndColumnNameMap;
    private List<Symbol> groupByKeys;
    public static boolean readLucene = false;

    @SuppressWarnings("unchecked")
	public HashAggregationOperator(
            OperatorContext operatorContext,
            List<Type> groupByTypes,
            List<Integer> groupByChannels,
            Step step,
            List<AccumulatorFactory> accumulatorFactories,
            Optional<Integer> hashChannel,
            int expectedGroups,
            Map<Symbol, Integer> outputMappings,
            Map<String, String> accumulatTypeAndColumnNameMap,
            List<Symbol> groupByKeys)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        requireNonNull(step, "step is null");
        requireNonNull(accumulatorFactories, "accumulatorFactories is null");
        requireNonNull(operatorContext, "operatorContext is null");

        this.groupByTypes = ImmutableList.copyOf(groupByTypes);
        this.groupByChannels = ImmutableList.copyOf(groupByChannels);
        this.accumulatorFactories = ImmutableList.copyOf(accumulatorFactories);
        this.hashChannel = requireNonNull(hashChannel, "hashChannel is null");
        this.step = step;
        this.expectedGroups = expectedGroups;
        this.types = toTypes(groupByTypes, step, accumulatorFactories, hashChannel);
        
        //added by cubeli
        this.outputMappings = outputMappings; 
        this.accumulatTypeAndColumnNameMap = accumulatTypeAndColumnNameMap;
        this.groupByKeys = groupByKeys;
        
    }
    
    

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public void finish()
    {
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        return finishing && aggregationBuilder == null && (outputIterator == null || !outputIterator.hasNext());
    }

    @Override
    public boolean needsInput()
    {
        return !finishing && outputIterator == null && (aggregationBuilder == null || !aggregationBuilder.isFull());
    }

    @Override
    public void addInput(Page page)
    {
        checkState(!finishing, "Operator is already finishing");
        requireNonNull(page, "page is null");
        if (aggregationBuilder == null) {
            aggregationBuilder = new GroupByHashAggregationBuilder(
                    accumulatorFactories,
                    step,
                    expectedGroups,
                    groupByTypes,
                    groupByChannels,
                    hashChannel,
                    operatorContext);

            // assume initial aggregationBuilder is not full
        }
        else {
            checkState(!aggregationBuilder.isFull(), "Aggregation buffer is full");
        }
        
        /*
        * modified by cubeli for lucene
        * if invoke processPage(page) every time, the partial accumulation result will be
        * accumulated, and the incorrect result will be got in the final accumulation
        */
//        if(!readLucene){
//        
//        	readLucene = true;
        	aggregationBuilder.processPage(page);        	
//        }

    }

//    @Override
//    public Page getOutput()
//    {
//        if (outputIterator == null || !outputIterator.hasNext()) {
//            // current output iterator is done
//            outputIterator = null;
//
//            // no data
//            if (aggregationBuilder == null) {
//                return null;
//            }
//
//            // only flush if we are finishing or the aggregation builder is full
//            if (!finishing && !aggregationBuilder.isFull()) {
//                return null;
//            }
//
//            outputIterator = aggregationBuilder.build();
//            aggregationBuilder = null;
//
//            if (!outputIterator.hasNext()) {
//                // current output iterator is done
//                outputIterator = null;
//                return null;
//            }
//        }
//
//        return outputIterator.next();
//    
//    }
    
	/*
	 * modified by cubeli
	 * the partial accumulation result will be got from read lucene index
	 * the final accumulation result will be got from presto
	 * 
	 */
    @Override
    public Page getOutput(){
    	
		if (step == Step.PARTIAL) {
			
			return getLucenePage4();
		} else {

			return getPrestoPage();
		}
    	
    }
    
	public Page getPrestoPage() {

		if (outputIterator == null || !outputIterator.hasNext()) {
			// current output iterator is done
			outputIterator = null;

			// no data
			if (aggregationBuilder == null) {
				return null;
			}

			// only flush if we are finishing or the aggregation builder is full
			if (!finishing && !aggregationBuilder.isFull()) {
				return null;
			}

			outputIterator = aggregationBuilder.build();
			aggregationBuilder = null;

			if (!outputIterator.hasNext()) {
				// current output iterator is done
				outputIterator = null;
				return null;
			}
		}

		return outputIterator.next();
	}

    private static List<Type> toTypes(List<? extends Type> groupByType, Step step, List<AccumulatorFactory> factories, Optional<Integer> hashChannel)
    {
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        types.addAll(groupByType);
        if (hashChannel.isPresent()) {
            types.add(BIGINT);
        }
        for (AccumulatorFactory factory : factories) {
            types.add(new Aggregator(factory, step).getType());
        }
        return types.build();
    }
    


    
    private  List<Pair<String, String>> getGroupByInfos(Map<Symbol, Integer> outputMappings, Map<String, String> accumulatTypeAndColumnNameMap, List<Symbol> groupByKeys){
    	
    	List<Pair<String, String>> returnList = new ArrayList<Pair<String, String>>();
    	
    	
    	for(int i = 0; i < groupByKeys.size(); i++){
    		
    		Pair<String, String> pair = new Pair<String, String>(groupByKeys.get(i).getName(), types.get(i).getTypeSignature().getBase());
    		returnList.add(pair);
    	}
    	
    	return returnList;
    }
    
    /*
     * added by cubeli for luecne 
     * this method is designed for multi-groupby
     * and it process the hash block as an normal block
     */
    private Page getLucenePage3(){
    	
       	List<List<String>> luceneOrderResult = newTestLuceneResult3();//should be replaced by getLuceneResult3()
    	int entryNum = luceneOrderResult.size();
    	
    	List<Pair<Type, BlockBuilder>> type2Bb = new ArrayList<Pair<Type, BlockBuilder>>();
    	for(Type type : types){
    		
    		BlockBuilder bb = HashAggregationOperator.getBlockbuilderFromType(type, entryNum);
    		type2Bb.add(new Pair<Type, BlockBuilder>(type, bb));
    	}
    	
    	int groupByKeySize = groupByKeys.size();
    	

    	for(int i = 0; i < types.size(); i++){
    		
    		
    		if(i == groupByKeySize){
    			
    			//TODO build this hash block ??
    		}
    		Type type = types.get(i);
			BlockBuilder blockBuilder = type2Bb.get(i).cud;
			for(List<String> line: luceneOrderResult){
				
				String accuValue = line.get(i);			
				if(type instanceof BigintType){
					
					long value = Long.parseLong(accuValue);
					BIGINT.writeLong(blockBuilder, value);
				}else if(type instanceof VarcharType){
					
					String value = accuValue;
					VARCHAR.writeString(blockBuilder, value);
				}else if(type instanceof DoubleType){
					
					Double value = Double.parseDouble(accuValue);
					DOUBLE.writeDouble(blockBuilder, value);
				}					
			}
    	}
    	
		BlockBuilder bb[] = new BlockBuilder[type2Bb.size()];
		int idx = 0;
		for(Pair<Type, BlockBuilder> pair : type2Bb){
			
			BlockBuilder blockBuilder = pair.cud;
			bb[idx++] = blockBuilder;
		}
		Page expectedPage = new Page(bb);
		finishing = true;
    	return expectedPage;
    }
    
    /*
     * added by cubeli
     * 该方法也用于产生一个page，但是参考的是RecordPageSource的getNextPage方法
     * 因为该方法可以很好的处理不同类型的数据,但是移植过来时去掉了很多异常处理
     */
    
    public class LuceneGroupByResult{
    	
    	Boolean getBoolean(int columnIdx){
    		
    		return null;
    	}
    	
    	Long getLong(int columnIdx){
    		
    		return null;
    	}
    	
    	Double getDouble(int columnIdx){
    		
    		return null;
    	}
    	
    	Slice getSlice(int columnIdx){
    		
    		return null;
    	}
    	
    	Object getObject(int columnIdx){
    		
    		return null;
    	}
    }
	private Page getLucenePage4(){
		
		LuceneGroupByResult lgbr = new LuceneGroupByResult();
		
		int groupByKeySize = groupByKeys.size();
		PageBuilder pageBuilder = new PageBuilder(types);
        for (int column = 0; column < types.size(); column++) {
        	
    		if(column == groupByKeySize){
    			
    			//TODO build this hash block
    		}
        	
            BlockBuilder output = pageBuilder.getBlockBuilder(column);

            Type type = types.get(column);
            Class<?> javaType = type.getJavaType();
            if (javaType == boolean.class) {
                type.writeBoolean(output, lgbr.getBoolean(column));
            }
            else if (javaType == long.class) {
                type.writeLong(output, lgbr.getLong(column));
            }
            else if (javaType == double.class) {
                type.writeDouble(output, lgbr.getDouble(column));
            }
            else if (javaType == Slice.class) {
                Slice slice = lgbr.getSlice(column);
                type.writeSlice(output, slice, 0, slice.length());
            }
            else {
                type.writeObject(output, lgbr.getObject(column));
            }
        }

        Page page = pageBuilder.build();
        pageBuilder.reset();

        return page;
    
	}
    
    List<List<String>> newTestLuceneResult3(){
    	
    	List<List<String>> rlList = new ArrayList<List<String>>();
    	List<String> l1 = new ArrayList<String>();
    	l1.add("one");l1.add("111");
    	
    	List<String> l2 = new ArrayList<String>();
    	l2.add("two");l2.add("222");
    	
    	List<String> l3 = new ArrayList<String>();
    	l3.add("three");l3.add("333");
    	
    	rlList.add(l1); rlList.add(l2); rlList.add(l3);

    	
    	return rlList;
    }
    
    List<List<String>> getLuceneResult3(){
    	
    	//groupby fields infos can be got from getGroupByInfos method
    	//return fields and accu types can be got from accumulatTypeAndColumnNameMap
    	return null;
    }
    
    @SuppressWarnings("null")
    /*
     * this method are designed for one group by key
     * and it has special processing for hash block
     */
	private Page getLucenePage2() {
    	
    	Page expectedPage = null;
    	
		List<Pair<String, List<String>>> luceneResultList = newTestLuceneResult2();
    	int entryNum = luceneResultList.size();
    	List<Pair<Type, BlockBuilder>> type2Bb = new ArrayList<Pair<Type, BlockBuilder>>() ;

    	int typeIdx = 0;
    	for(Type type : types){
    		
    		if(typeIdx++ == 1){
    			
    			BlockBuilder hashBb = BIGINT.createBlockBuilder(new BlockBuilderStatus(), entryNum);
    			type2Bb.add(new Pair<Type, BlockBuilder>(BIGINT, hashBb));
    			continue;
    		}
    		BlockBuilder bb = getBlockbuilderFromType(type, entryNum);
    		type2Bb.add(new Pair<Type, BlockBuilder>(type, bb));
    	}
    		

		
		//1.write groupby value
		Type groupByKeyType = type2Bb.get(0).cur;
		BlockBuilder groupByKeyBb = type2Bb.get(0).cud;
		for(Pair<String, List<String>> pair : luceneResultList){
			
			if(groupByKeyType instanceof BigintType){
				
				long value = Long.parseLong(pair.cur);
				BIGINT.writeLong(groupByKeyBb, value);
			}else if(groupByKeyType instanceof VarcharType){
				
				String value = pair.cur; 
				VARCHAR.writeString(groupByKeyBb, value);
			}else if(groupByKeyType instanceof DoubleType){
				
				Double value = Double.parseDouble(pair.cur);
				DOUBLE.writeDouble(groupByKeyBb, value);
			}
		}
		
		//2.write hash value
		BlockBuilder hashBb = type2Bb.get(1).cud;
		for(int i = 0; i < luceneResultList.size(); i++){
			
			BIGINT.writeLong(hashBb, i);
		}
		
		//3.write other column
		for(int i = 2; i < types.size(); i++){
			
			Type type = types.get(i);
			BlockBuilder blockBuilder = type2Bb.get(i).cud;
			for(Pair<String, List<String>> pair: luceneResultList){
				
				String accuValue = pair.cud.get(i-2);			
				if(type instanceof BigintType){
					
					long value = Long.parseLong(accuValue);
					BIGINT.writeLong(blockBuilder, value);
				}else if(type instanceof VarcharType){
					
					String value = accuValue;
					VARCHAR.writeString(blockBuilder, value);
				}else if(type instanceof DoubleType){
					
					Double value = Double.parseDouble(accuValue);
					DOUBLE.writeDouble(blockBuilder, value);
				}					
			}
		}

		BlockBuilder bb[] = new BlockBuilder[type2Bb.size()];
		int idx = 0;
		for(Pair<Type, BlockBuilder> pair : type2Bb){
			
			BlockBuilder blockBuilder = pair.cud;
			bb[idx++] = blockBuilder;
		}
		expectedPage = new Page(bb);
		
		finishing = true;
    	return expectedPage;
    	
    }
    
    List<Pair<String, List<String>>> newTestLuceneResult2(){
    	
    	List<Pair<String, List<String>>> rlList = new ArrayList<Pair<String, List<String>>>();
    	
    	List<String> l1 = new ArrayList<String>();
    	l1.add("111");l1.add("111.1");l1.add("1111");
    	
    	List<String> l2 = new ArrayList<String>();
    	l2.add("222");l2.add("222.2");l2.add("222.22");
    
    	List<String> l3 = new ArrayList<String>();
    	l3.add("333");l3.add("333.3");l1.add("3333");
    	
       
    	
    	Pair<String, List<String>> p1 = new Pair<String, List<String>>("cubeli", l1);
    	Pair<String, List<String>> p2 = new Pair<String, List<String>>("yangziying", l2);
    	Pair<String, List<String>> p3 = new Pair<String, List<String>>("litairen", l3);
    	
    	rlList.add(p1);
    	rlList.add(p2);
    	rlList.add(p3);
    	
    	return rlList;
    }
    
    public  static BlockBuilder getBlockbuilderFromType(Type type, int entryNum) {
    	
    	if(type instanceof BigintType){
    		
    		return BIGINT.createBlockBuilder(new BlockBuilderStatus(), entryNum);
    	}else if(type instanceof VarcharType){
    		
    		return VARCHAR.createBlockBuilder(new BlockBuilderStatus(), entryNum);
    	}else if(type instanceof DoubleType){
			
    		return DOUBLE.createBlockBuilder(new BlockBuilderStatus(), entryNum);
		}
    	else{
    		
    		return null;
    	}
    }
    
   
    //for testing
	private Page getLucenePage() {

		Page expectedPage = null;
		try {
			Map<String, Long> map = getCountResult();
			int expectedEntryNum = map.size();

			BlockBuilder fieldBlockBuilder = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), expectedEntryNum);
			BlockBuilder hashBlockBuilder = BIGINT.createBlockBuilder(new BlockBuilderStatus(), expectedEntryNum);
			BlockBuilder countBlockBuilder = BIGINT.createBlockBuilder(new BlockBuilderStatus(), expectedEntryNum);

			int count = 0;
			for (Entry<String, Long> entry : map.entrySet()) {

				VARCHAR.writeString(fieldBlockBuilder, entry.getKey());
				BIGINT.writeLong(hashBlockBuilder, count++);
				BIGINT.writeLong(countBlockBuilder, entry.getValue());
			}

			expectedPage = new Page(fieldBlockBuilder, hashBlockBuilder, countBlockBuilder);

		} catch (IOException e) {
			e.printStackTrace();
		}

		finishing = true;
		return expectedPage;
	}
    
	

    private Map<String, List<String>> getLuceneResult() throws IOException{
    		
    	IndexReader reader = null;
    	Map<String,List<String>> returnMap = new HashMap<String, List<String>>();
    	
    	String groupByColumnName = groupByKeys.get(0).getName();
    	
		try {
			reader = DirectoryReader.open(FSDirectory.open(Paths.get("/home/liyong/workspace-neno/lucenetest/index")));
		} catch (IOException e)
		{
			e.printStackTrace();
		}
		IndexSearcher searcher = new IndexSearcher(reader);
		
		Terms terms = MultiFields.getTerms(searcher.getIndexReader(), groupByColumnName);
		TermsEnum te = terms.iterator();
		while(te.next() != null){
			
			List<String> l = new ArrayList<String>();
			String name = te.term().utf8ToString();
			int count = te.docFreq();
			l.add(String.valueOf(count));
			returnMap.put(name, l);
		}
		
		return returnMap;
    }
    
    
	//for testiing
    private Map<String, Long> getCountResult() throws IOException{
    	
    	IndexReader reader = null;
    	Map<String,Long> returnMap = new HashMap<String, Long>();
		try {
			reader = DirectoryReader.open(FSDirectory.open(Paths.get("/home/liyong/workspace-neno/lucenetest/index")));
		} catch (IOException e)
		{
			e.printStackTrace();
		}
		IndexSearcher searcher = new IndexSearcher(reader);
		
		Terms terms = MultiFields.getTerms(searcher.getIndexReader(), "orderpriority");
		TermsEnum te = terms.iterator();
		while(te.next() != null){
			
			String name = te.term().utf8ToString();
			int count = te.docFreq();
			returnMap.put(name, Long.valueOf(count));
		}
		
		return returnMap;
    }
    
	private Page newLuceneCountPageForTest() {

		// test1 start======================
		BlockBuilder varcharBlockBuilder = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), 5);
		VARCHAR.writeString(varcharBlockBuilder, "2-HIGH");
		VARCHAR.writeString(varcharBlockBuilder, "5-LOW");
		VARCHAR.writeString(varcharBlockBuilder, "1-URGENT");
		VARCHAR.writeString(varcharBlockBuilder, "4-NOT SPECIFIED");
		VARCHAR.writeString(varcharBlockBuilder, "3-MEDIUM");
		Block expectedBlock = varcharBlockBuilder.build();

		BlockBuilder longBlockBuilder1 = BIGINT.createBlockBuilder(new BlockBuilderStatus(), 5);
		BIGINT.writeLong(longBlockBuilder1, 100);
		BIGINT.writeLong(longBlockBuilder1, 101);
		BIGINT.writeLong(longBlockBuilder1, 102);
		BIGINT.writeLong(longBlockBuilder1, 103);
		BIGINT.writeLong(longBlockBuilder1, 104);

		// BlockBuilder longBlockBuilder2 = BIGINT.createBlockBuilder(new
		// BlockBuilderStatus(), 5);
		// BIGINT.writeLong(longBlockBuilder2, 300091);
		// BIGINT.writeLong(longBlockBuilder2, 300589);
		// BIGINT.writeLong(longBlockBuilder2, 300343);
		// BIGINT.writeLong(longBlockBuilder2, 300254);
		// BIGINT.writeLong(longBlockBuilder2, 298723);

		// for sum(totalprice)
		BlockBuilder doubleBlockBuilder2 = DOUBLE.createBlockBuilder(new BlockBuilderStatus(), 5);
		DOUBLE.writeDouble(doubleBlockBuilder2, 2.3);
		DOUBLE.writeDouble(doubleBlockBuilder2, 2.3);
		DOUBLE.writeDouble(doubleBlockBuilder2, 3.4);
		DOUBLE.writeDouble(doubleBlockBuilder2, 4.5);
		DOUBLE.writeDouble(doubleBlockBuilder2, 5.6);

		Page expectedPage = new Page(varcharBlockBuilder, longBlockBuilder1, doubleBlockBuilder2);
		finishing = true;
		return expectedPage;
	}

    private static class GroupByHashAggregationBuilder
    {
        private final GroupByHash groupByHash;
        private final List<Aggregator> aggregators;
        private final OperatorContext operatorContext;
        private final boolean partial;

        private GroupByHashAggregationBuilder(
                List<AccumulatorFactory> accumulatorFactories,
                Step step,
                int expectedGroups,
                List<Type> groupByTypes,
                List<Integer> groupByChannels,
                Optional<Integer> hashChannel,
                OperatorContext operatorContext)
        {
            this.groupByHash = createGroupByHash(operatorContext.getSession(), groupByTypes, Ints.toArray(groupByChannels), hashChannel, expectedGroups);
            this.operatorContext = operatorContext;
            this.partial = step.isOutputPartial();

            // wrapper each function with an aggregator
            ImmutableList.Builder<Aggregator> builder = ImmutableList.builder();
            requireNonNull(accumulatorFactories, "accumulatorFactories is null");
            for (int i = 0; i < accumulatorFactories.size(); i++) {
                AccumulatorFactory accumulatorFactory = accumulatorFactories.get(i);
                builder.add(new Aggregator(accumulatorFactory, step));
            }
            aggregators = builder.build();
        }

        private void processPage(Page page)
        {
            if (aggregators.isEmpty()) {
                groupByHash.addPage(page);
                return;
            }

            GroupByIdBlock groupIds = groupByHash.getGroupIds(page);

            for (Aggregator aggregator : aggregators) {
                aggregator.processPage(groupIds, page);
            }
        }

        public boolean isFull()
        {
            long memorySize = groupByHash.getEstimatedSize();
            for (Aggregator aggregator : aggregators) {
                memorySize += aggregator.getEstimatedSize();
            }
            memorySize -= operatorContext.getOperatorPreAllocatedMemory().toBytes();
            if (memorySize < 0) {
                memorySize = 0;
            }
            if (partial) {
                return !operatorContext.trySetMemoryReservation(memorySize);
            }
            else {
                operatorContext.setMemoryReservation(memorySize);
                return false;
            }
        }

        public Iterator<Page> build()
        {
            List<Type> types = new ArrayList<>(groupByHash.getTypes());
            for (Aggregator aggregator : aggregators) {
                types.add(aggregator.getType());
            }

            final PageBuilder pageBuilder = new PageBuilder(types);
            return new AbstractIterator<Page>()
            {
                private final int groupCount = groupByHash.getGroupCount();
                private int groupId;

                @Override
                protected Page computeNext()
                {
                    if (groupId >= groupCount) {
                        return endOfData();
                    }

                    pageBuilder.reset();

                    List<Type> types = groupByHash.getTypes();
                    while (!pageBuilder.isFull() && groupId < groupCount) {
                        groupByHash.appendValuesTo(groupId, pageBuilder, 0);

                        pageBuilder.declarePosition();
                        for (int i = 0; i < aggregators.size(); i++) {
                            Aggregator aggregator = aggregators.get(i);
                            BlockBuilder output = pageBuilder.getBlockBuilder(types.size() + i);
                            aggregator.evaluate(groupId, output);
                        }

                        groupId++;
                    }

                    return pageBuilder.build();
                }
            };
        }
    }

    private static class Aggregator
    {
        private final GroupedAccumulator aggregation;
        private final Step step;
        private final int intermediateChannel;

        private Aggregator(AccumulatorFactory accumulatorFactory, Step step)
        {
            if (step.isInputRaw()) {
                intermediateChannel = -1;
                aggregation = accumulatorFactory.createGroupedAccumulator();
            }
            else {
                checkArgument(accumulatorFactory.getInputChannels().size() == 1, "expected 1 input channel for intermediate aggregation");
                intermediateChannel = accumulatorFactory.getInputChannels().get(0);
                aggregation = accumulatorFactory.createGroupedIntermediateAccumulator();
            }
            this.step = step;
        }

        public long getEstimatedSize()
        {
            return aggregation.getEstimatedSize();
        }

        public Type getType()
        {
            if (step.isOutputPartial()) {
                return aggregation.getIntermediateType();
            }
            else {
                return aggregation.getFinalType();
            }
        }

        public void processPage(GroupByIdBlock groupIds, Page page)
        {
            if (step.isInputRaw()) {
                aggregation.addInput(groupIds, page);
            }
            else {
                aggregation.addIntermediate(groupIds, page.getBlock(intermediateChannel));
            }
        }

        public void evaluate(int groupId, BlockBuilder output)
        {
            if (step.isOutputPartial()) {
                aggregation.evaluateIntermediate(groupId, output);
            }
            else {
                aggregation.evaluateFinal(groupId, output);
            }
        }
    }
}
