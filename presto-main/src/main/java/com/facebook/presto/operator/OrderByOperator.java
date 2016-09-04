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

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.sun.xml.internal.xsom.impl.scd.Iterators.Map;

import java.util.List;

import org.apache.lucene.queryparser.ext.Extensions.Pair;
<<<<<<< Updated upstream
=======
//import org.jboss.netty.handler.codec.serialization.ObjectEncoderOutputStream;
>>>>>>> Stashed changes

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.HashMap;

public class OrderByOperator
        implements Operator
{
    public static class OrderByOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> sourceTypes;
        private final List<Integer> outputChannels;
        private final int expectedPositions;
        private final List<Integer> sortChannels;
        private final List<SortOrder> sortOrder;
        private final List<Type> types;
        private boolean closed;
        
        //added by cubeli for lucene
        private List<Symbol> outputs;

        public OrderByOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> sourceTypes,
                List<Integer> outputChannels,
                int expectedPositions,
                List<Integer> sortChannels,
                List<SortOrder> sortOrder,
                List<Symbol> outputs)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceTypes = ImmutableList.copyOf(requireNonNull(sourceTypes, "sourceTypes is null"));
            this.outputChannels = requireNonNull(outputChannels, "outputChannels is null");
            this.expectedPositions = expectedPositions;
            this.sortChannels = ImmutableList.copyOf(requireNonNull(sortChannels, "sortChannels is null"));
            this.sortOrder = ImmutableList.copyOf(requireNonNull(sortOrder, "sortOrder is null"));

            this.types = toTypes(sourceTypes, outputChannels);
            this.outputs = outputs;
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

            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, OrderByOperator.class.getSimpleName());
            return new OrderByOperator(
                    operatorContext,
                    sourceTypes,
                    outputChannels,
                    expectedPositions,
                    sortChannels,
                    sortOrder,
                    outputs);
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new OrderByOperatorFactory(operatorId, planNodeId, sourceTypes, outputChannels, expectedPositions, sortChannels, sortOrder,outputs);
        }
    }

    private enum State
    {
        NEEDS_INPUT,
        HAS_OUTPUT,
        FINISHED
    }

    private final OperatorContext operatorContext;
    private final List<Integer> sortChannels;
    private final List<SortOrder> sortOrder;
    private final int[] outputChannels;
    private final List<Type> types;

    private final PagesIndex pageIndex;

    private final PageBuilder pageBuilder;
    private int currentPosition;

    private State state = State.NEEDS_INPUT;
    
    //added by cubeli for lucene
    private List<Symbol> outputs;

    public OrderByOperator(
            OperatorContext operatorContext,
            List<Type> sourceTypes,
            List<Integer> outputChannels,
            int expectedPositions,
            List<Integer> sortChannels,
            List<SortOrder> sortOrder,
            List<Symbol> outputs)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.outputChannels = Ints.toArray(requireNonNull(outputChannels, "outputChannels is null"));
        this.types = toTypes(sourceTypes, outputChannels);
        this.sortChannels = ImmutableList.copyOf(requireNonNull(sortChannels, "sortChannels is null"));
        this.sortOrder = ImmutableList.copyOf(requireNonNull(sortOrder, "sortOrder is null"));

        this.pageIndex = new PagesIndex(sourceTypes, expectedPositions);

        this.pageBuilder = new PageBuilder(this.types);
        this.outputs = outputs;
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
        if (state == State.NEEDS_INPUT) {
            state = State.HAS_OUTPUT;

            // sort the index
            pageIndex.sort(sortChannels, sortOrder);
        }
    }

    @Override
    public boolean isFinished()
    {
        return state == State.FINISHED;
    }

    @Override
    public boolean needsInput()
    {
        return state == State.NEEDS_INPUT;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(state == State.NEEDS_INPUT, "Operator is already finishing");
        requireNonNull(page, "page is null");

        pageIndex.addPage(page);
        operatorContext.setMemoryReservation(pageIndex.getEstimatedSize().toBytes());
    }

    @Override
    public Page getOutput()
    {
 /*       if (state != State.HAS_OUTPUT) {
            return null;
        }

        if (currentPosition >= pageIndex.getPositionCount()) {
            state = State.FINISHED;
            return null;
        }

        // iterate through the positions sequentially until we have one full page
        pageBuilder.reset();
        currentPosition = pageIndex.buildPage(currentPosition, outputChannels, pageBuilder);

        // output the page if we have any data
        if (pageBuilder.isEmpty()) {
            state = State.FINISHED;
            return null;
        }

        Page page = pageBuilder.build();
        return page;*/
    	
//    	return newLucenePage();
    	
    	return getLucenePage2();
    	
    }

    
    Page getLucenePage2(){
    	
    	
    	
    	List<List<String>> luceneOrderResult = newTestLuceneResult();
    	int entryNum = luceneOrderResult.size();
    	
    	List<Pair<Type, BlockBuilder>> type2Bb = new ArrayList<Pair<Type, BlockBuilder>>();
    	for(Type type : types){
    		
    		BlockBuilder bb = HashAggregationOperator.getBlockbuilderFromType(type, entryNum);
    		type2Bb.add(new Pair<Type, BlockBuilder>(type, bb));
    	}
    	

    	for(int i = 0; i < types.size(); i++){
    		
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
		state = State.FINISHED;
    	return expectedPage;
    	
    }
    
    
    List<List<String>> newTestLuceneResult(){
    	
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
    
    //for testing
    private Page newLucenePage() {
		
    	//test1 start======================
//        BlockBuilder varcharBlockBuilder = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), 5);
//        VARCHAR.writeString(varcharBlockBuilder, "2-HIGH");
//        VARCHAR.writeString(varcharBlockBuilder, "5-LOW");
//        VARCHAR.writeString(varcharBlockBuilder, "1-URGENT");
//        VARCHAR.writeString(varcharBlockBuilder, "4-NOT SPECIFIED");
//        VARCHAR.writeString(varcharBlockBuilder, "3-MEDIUM");
//        Block expectedBlock = varcharBlockBuilder.build();
        
        BlockBuilder longBlockBuilder1 = BIGINT.createBlockBuilder(new BlockBuilderStatus(), 5);
        BIGINT.writeLong(longBlockBuilder1, 0);
        BIGINT.writeLong(longBlockBuilder1, 1);
        BIGINT.writeLong(longBlockBuilder1, 1);
        BIGINT.writeLong(longBlockBuilder1, 5);
        BIGINT.writeLong(longBlockBuilder1, 7);  
        
        
        BlockBuilder longBlockBuilder2 = BIGINT.createBlockBuilder(new BlockBuilderStatus(), 5);
        BIGINT.writeLong(longBlockBuilder2, 5);
        BIGINT.writeLong(longBlockBuilder2, 2);
        BIGINT.writeLong(longBlockBuilder2, 1);
        BIGINT.writeLong(longBlockBuilder2, 5);
        BIGINT.writeLong(longBlockBuilder2, 7);  
        

        Page expectedPage = new Page(longBlockBuilder1, longBlockBuilder2);
        state = State.FINISHED;
        return expectedPage;
	}

	private static List<Type> toTypes(List<? extends Type> sourceTypes, List<Integer> outputChannels)
    {
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (int channel : outputChannels) {
            types.add(sourceTypes.get(channel));
        }
        return types.build();
    }
}
