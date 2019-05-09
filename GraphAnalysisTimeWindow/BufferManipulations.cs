using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphAnalysisTimeWindow
{
    public static class BufferManipulations
    {
        /// <summary>
        /// Inputs:
        ///     bufferMinutes: Groups together states (State) separated by equal or few minutes (NodeDt) of the same PatientId
        /// Implied inputs: 
        ///     Query: select PatientChronoId, PatientId, NodeType, NodeDt from Fsm.Fsm order by PatientId, PatientChronoId
        ///     Columns: PatientChronoId (PK), PatientId, NodeType, NodeDt
        /// Outputs:
        ///     Tables: FSM.FsmBuffered, FSM.GroupedStates
        /// </summary>
        /// <param name="conString"></param>
        public static void MapDataWithBuffer(string query, int bufferMinutes, string conString, string outputTable1, string outputTable2)
        {
            //Get data
            var dt = SqlTable.GetTable(query, conString);

            //Constants            
            int newStateCounter = Int32.MinValue;
            foreach (var row in dt.AsEnumerable())
            {
                int stateCounter = Int32.Parse(row["NodeType"].ToString());
                newStateCounter = Math.Max(newStateCounter, stateCounter);
            }

            //Create output tables
            var outputBuffered = new DataTable();
            outputBuffered.Columns.Add(new DataColumn("PatientChronoId", typeof(int)));
            outputBuffered.Columns.Add(new DataColumn("PatientId", typeof(int)));
            outputBuffered.Columns.Add(new DataColumn("NodeType", typeof(string)));

            //New state
            var outputMapDict = new Dictionary<string, string>(); //The key string is a comma separated list of integers from "states" variables

            //Fill output tables
            var fsmIdCounter = Int32.Parse(dt.Rows[0]["PatientId"].ToString());
            var stateDtCounter = DateTime.Parse(dt.Rows[0]["NodeDt"].ToString()); ;
            var firstChronoId = Int32.Parse(dt.Rows[0]["PatientChronoId"].ToString());
            var states = new List<string>();
            foreach (var row in dt.AsEnumerable())
            {
                var chronoId = Int32.Parse(row["PatientChronoId"].ToString());
                var fsmId = Int32.Parse(row["PatientId"].ToString());
                var state = row["NodeType"].ToString();
                var stateDt = DateTime.Parse(row["NodeDt"].ToString());
                bool isSameFsmId = fsmIdCounter == fsmId;
                bool isInBuffer = stateDt.Subtract(stateDtCounter).TotalMinutes <= bufferMinutes;

                //Different scenarios
                if (isSameFsmId && isInBuffer)
                {
                    //(Do not add buffer to output)
                    //Add this row to the buffer 
                    states.Add(state);
                    stateDtCounter = stateDt;
                }
                else if (isSameFsmId && !isInBuffer)
                {
                    firstChronoId = AddBufferToOutputAndReset(ref newStateCounter, outputBuffered, outputMapDict, fsmIdCounter, firstChronoId, states, chronoId);

                    //Add this row to the buffer.
                    states.Add(state);
                    stateDtCounter = stateDt;
                }
                else if (!isSameFsmId) //Not the same person
                {
                    firstChronoId = AddBufferToOutputAndReset(ref newStateCounter, outputBuffered, outputMapDict, fsmIdCounter, firstChronoId, states, chronoId);

                    //Add this row to the buffer.
                    fsmIdCounter = fsmId;
                    states.Add(state);
                    stateDtCounter = stateDt;
                }
            }
            AddBufferToOutputAndReset(ref newStateCounter, outputBuffered, outputMapDict, fsmIdCounter, firstChronoId, states, -1);

            //Save to database
            SqlTable.ExecuteNonQuery(conString, String.Format(Resources.Resource.CreateOutputTables, outputTable1, outputTable2));
            SqlTable.BulkInsertDataTable(conString, outputTable1, outputBuffered);
            SqlTable.BulkInsertDataTable(conString, outputTable2, ConvertMapStatesToDt(outputMapDict));
        }

        private static DataTable ConvertMapStatesToDt(Dictionary<string,string> outputMapDict)
        {
            var outputMapStates = new DataTable();
            outputMapStates.Columns.Add(new DataColumn("StateInitial", typeof(string)));
            outputMapStates.Columns.Add(new DataColumn("StateFinal", typeof(string)));

            foreach (var keyvalue in outputMapDict)
            {
                var initialStates = keyvalue.Key.Split(new string[] { ", " }, StringSplitOptions.RemoveEmptyEntries);
                foreach (var initialState in initialStates)
                {
                    var newRow = outputMapStates.NewRow();
                    newRow["StateInitial"] = initialState;
                    newRow["StateFinal"] = keyvalue.Value;
                    outputMapStates.Rows.Add(newRow);
                }
            }
            SqlTable.AddIdCol(outputMapStates);
            return outputMapStates;
        }

        private static int AddBufferToOutputAndReset(ref int newStateCounter, DataTable outputBuffered, Dictionary<string, string> outputMapDict, int fsmIdCounter, int firstChronoId, List<string> states, int chronoId)
        {
            //Add buffer to output.  
            var newRow = outputBuffered.NewRow();
            newRow["PatientChronoId"] = firstChronoId;
            newRow["PatientId"] = fsmIdCounter;
            newRow["NodeType"] = GetState(states, ref newStateCounter, outputMapDict);
            outputBuffered.Rows.Add(newRow);

            //Reset buffer.
            firstChronoId = chronoId;
            states.Clear();
            return firstChronoId;
        }

        private static string GetState(List<string> states, ref int newStateCounter, Dictionary<string, string> outputMapDict)
        {
            var statesDistinctOrdered = states.Distinct().OrderBy(x => x).ToList();

            //No new state needed
            if (statesDistinctOrdered.Count == 1)
                return statesDistinctOrdered[0];

            //New state required
            var key = String.Join(", ", statesDistinctOrdered);
            if (outputMapDict.ContainsKey(key))
                return outputMapDict[key];

            //Add new state
            newStateCounter++;
            outputMapDict.Add(key, newStateCounter.ToString());
            return newStateCounter.ToString();
        }
    }
}
