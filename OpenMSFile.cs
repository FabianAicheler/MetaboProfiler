using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OpenMS.OpenMSFile
{
    public class OpenMSFile
    {
        private String file;

        public OpenMSFile() {}

        public OpenMSFile(string file)
        {
            this.file = file;
        }

        public String get_name()
        {
            return this.file;
        }
    }

    //custom OpenMS file classes for Results
    public class mzTabFile
    {
        private String file;

        public mzTabFile(string file)
        {
            this.file = file;
        }

        public String get_name()
        {
            return this.file;
        }
    }

    public class mzMLFile
    {
        private String file;

        public mzMLFile(string file)
        {
            this.file = file;
        }

        public String get_name()
        {
            return this.file;
        }
    }



    public class ConsensusXMLFile
    {
        private String file;

        public ConsensusXMLFile(string file)
        {
            this.file = file;
        }

        public String get_name()
        {
            return this.file;
        }
    }



    public class featureXMLFile
    {
        private String file;

        public featureXMLFile(string file)
        {
            this.file = file;
        }

        public String get_name()
        {
            return this.file;
        }
    }

    

}
