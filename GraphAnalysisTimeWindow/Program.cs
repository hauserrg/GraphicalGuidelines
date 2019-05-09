using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphAnalysisTimeWindow
{
    class Program
    {
        static void Main(string[] args)
        {
            //Data base connection string
			var conString = "Data Source=DESKTOP-1HTEBRI; Initial Catalog=Dev; Integrated Security=True;";

            //Query to gather input data.  Requires NodeId, PatientId, NodeType, NodeDateTime
            var query = "select PatientChronoId, PatientId, NodeType, NodeDt from GraphAnalysis.NodesWithPatientChronoId order by PatientId, PatientChronoId";

            //Number of minutes to group together nodes in close proximatity
            int timeWindow = 1440 * 7; //7 days

            //Output schema.table name

            //Output schema.table name
            string outputTable1 = "GraphAnalysis.BufferedTime";
            string outputTable2 = "GraphAnalysis.GroupedStates";

            BufferManipulations.MapDataWithBuffer(query, timeWindow, conString, outputTable1, outputTable2); //output = FSM.FsmBuffered, FSM.GroupedStates
        }
    }
}
