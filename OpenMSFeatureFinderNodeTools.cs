using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Xml;
using Thermo.Magellan.BL.Data;
using Thermo.Magellan.BL.Processing;
using Thermo.Magellan.BL.Processing.Interfaces;
using Thermo.Magellan.DataLayer.FileIO;
using Thermo.Magellan.EntityDataFramework;
using Thermo.Magellan.Exceptions;
using Thermo.Magellan.MassSpec;
using Thermo.Magellan.Utilities;
using Thermo.Metabolism.Algorithms;
using Thermo.Metabolism.DataObjects;
using Thermo.Metabolism.DataObjects.EntityDataObjects;
using Thermo.Metabolism.DataObjects.PeakModels;
//using Thermo.Magellan.BL.ReportEntityData; //for own table
//using Thermo.Metabolism.DataObjects.EntityDataObjects;


using OpenMS.OpenMSFile;
using Thermo.Metabolism.Processing.Services.Interfaces;
using Thermo.Metabolism.DataObjects.Constants;

namespace OpenMS.AdapterNodes
{

    public partial class OpenMSFeatureFinderNode : ProcessingNode<UnknownFeatureConsolidationProvider, ConsensusXMLFile>,
        IResultsSink<MassSpectrumCollection>
	{

        #region stuff_for_tools

        private void create_default_ini(string execPath, string ini_path)
        {
            var timer = Stopwatch.StartNew();

            var ini_loc = String.Format("\"{0}\"", ini_path);
            //SendAndLogMessage(execPath + NodeScratchDirectory + ini_loc);

            var process = new Process
            {
                StartInfo =
                {
                    FileName = execPath,
                    WorkingDirectory = NodeScratchDirectory,
                    Arguments = " -write_ini " + String.Format("\"{0}\"", ini_path),
                    UseShellExecute = false,
                    RedirectStandardOutput = false,
                    CreateNoWindow = false
                }
            };

            try
            {
                var openMS_shared_dir = Path.Combine(ServerConfiguration.ToolsDirectory, "OpenMS-2.0/share/OpenMS");
                process.StartInfo.EnvironmentVariables["OPENMS_DATA_PATH"] = openMS_shared_dir;
                process.Start();

                try
                {
                    process.Refresh();
                    //process.PriorityClass = ProcessPriorityClass.BelowNormal;//Do not use, causes exception!
                    process.WaitForExit();
                }
                catch (InvalidOperationException ex)
                {
                    NodeLogger.ErrorFormat(ex, "The following exception is raised during the execution of \"{0}\":", execPath);
                    throw;
                }

                if (process.ExitCode != 0)
                {
                    throw new MagellanProcessingException(
                        String.Format(
                            "The exit code of {0} was {1}. (The expected exit code is 0)",
                            Path.GetFileName(process.StartInfo.FileName), process.ExitCode));
                }
            }
            catch (System.Threading.ThreadAbortException)
            {
                throw;
            }
            catch (Exception ex)
            {
                NodeLogger.ErrorFormat(ex, "The following exception is raised during the execution of \"{0}\":", execPath);
                throw;
            }
            finally
            {
                if (!process.HasExited)
                {
                    NodeLogger.WarnFormat(
                        "The process [{0}] isn't finished correctly -> force the process to exit now", process.StartInfo.FileName);
                    process.Kill();
                }
            }
        }

        private void WriteItem(string ini_path, Dictionary<string, string> parameters)
        {
            XmlDocument doc = new XmlDocument();
            doc.Load(ini_path);
            XmlNodeList nlist = doc.GetElementsByTagName("ITEM");
            foreach (XmlNode item in nlist)
            {
                foreach (string param in parameters.Keys)
                {
                    if (item.Attributes["name"].Value == param)
                    {
                        item.Attributes["value"].Value = parameters[param];
                    }
                }
            }
            //doc.Save(Path.Combine(NodeScratchDirectory, "ToolParameters.xml"));
            doc.Save(ini_path);
        }

        //Write ITEMLISTs, used for input or output file lists
        private static void replaceItemList(string[] vars, string ini_path, string mode)
        {
            XmlDocument doc = new XmlDocument();
            doc.Load(ini_path);
            XmlNodeList itemlist = doc.GetElementsByTagName("ITEMLIST");
            foreach (XmlNode item in itemlist)
            {
                //mode: in or out?
                if (item.Attributes["name"].Value == mode)
                {
                    //we want to remove e.g. H+:0.9
                    //XmlAttributeCollection attributes = item.Attributes.;
                    //item.RemoveAll();
                    //((XmlElement) item).IsEmpty = true;
                    while (item.FirstChild != null){
                        item.RemoveChild(item.FirstChild);
                    }
                    foreach (var fn in vars)
                    {
                        //We add LISTITEMS to until then empty ITEMLISTS
                        var listitem = doc.CreateElement("LISTITEM");
                        XmlAttribute newAttribute = doc.CreateAttribute("value");
                        newAttribute.Value = fn;
                        listitem.SetAttributeNode(newAttribute);
                        item.AppendChild(listitem);                        
                    }
                }
            }
            doc.Save(ini_path);
        }

        //Write mz and rt parameters. different function than WriteItem due to specific structure in considered tools
        private void write_MZ_RT_thresholds(string ini_path)
        {
            XmlDocument doc = new XmlDocument();
            doc.Load(ini_path);
            XmlNodeList nlist = doc.GetElementsByTagName("ITEM");
            foreach (XmlNode item in nlist)
            {
                if ((item.ParentNode.Attributes["name"].Value == "distance_MZ") && (item.Attributes["name"].Value == "max_difference"))
                {
                    var mzthresh = MZThreshold.ToString();
                    item.Attributes["value"].Value = mzthresh;
                }
                else if ((item.ParentNode.Attributes["name"].Value == "distance_MZ") && (item.Attributes["name"].Value == "unit"))
                {
                    //always use ppm
                    item.Attributes["value"].Value = "ppm";
                }
                else if ((item.ParentNode.Attributes["name"].Value == "distance_RT") && (item.Attributes["name"].Value == "max_difference"))
                {
                    item.Attributes["value"].Value = (RTThreshold.Value * 60).ToString(); //need to convert minute(CD) to seconds(OpenMS)!
                }
            }
            doc.Save(ini_path);
        }

        //execute specific OpenMS Tool (execPath) with specified Ini (ParamPath)        
        private void RunTool(string execPath, string ParamPath)
        {

            var timer = Stopwatch.StartNew();

            var process = new Process
            {
                StartInfo =
                {
                    FileName = execPath,
                    WorkingDirectory = NodeScratchDirectory,
                    //WorkingDirectory = FileHelper.GetShortFileName(NodeScratchDirectory), 

                    Arguments = " -ini " + String.Format("\"{0}\"",ParamPath),
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    CreateNoWindow = false,
                    //WindowStyle = ProcessWindowStyle.Hidden
                }
            };

            var openMS_shared_dir = Path.Combine(ServerConfiguration.ToolsDirectory, "OpenMS-2.0/share/OpenMS");
            process.StartInfo.EnvironmentVariables["OPENMS_DATA_PATH"] = openMS_shared_dir;

            SendAndLogTemporaryMessage("Starting process [{0}] in working directory [{1}] with arguments [{2}]",
                            process.StartInfo.FileName, process.StartInfo.WorkingDirectory, process.StartInfo.Arguments);

            WriteLogMessage(MessageLevel.Debug,
                            "Starting process [{0}] in working directory [{1}] with arguments [{2}]",
                            process.StartInfo.FileName, process.StartInfo.WorkingDirectory, process.StartInfo.Arguments);
                        
            try
            {
                process.Start();

                try
                {
                    //process.PriorityClass = ProcessPriorityClass.BelowNormal;

                    string current_work = "";
                    while (process.HasExited == false)
                    {
                        var output = process.StandardOutput.ReadLine();


                        // move on if no new announcement. 
                        if (String.IsNullOrEmpty(output))
                        {
                            continue;
                        }

                        //store all results (for now?) of OpenMS Tool output
                        WriteLogMessage(MessageLevel.Debug, output);

                        // Parse the output and report progress using the method SendAndLogTemporaryMessage
                        if (output.Contains(@"Progress of 'loading mzML file':"))
                        {
                            current_work = "Progress of 'loading mzML file':";
                        }
                        else if (output.Contains("Progress of 'loading chromatograms':"))
                        {
                            current_work = "Progress of 'loading chromatograms':";
                        }
                        else if (output.Contains("Progress of 'mass trace detection':"))
                        {
                            current_work = "Progress of 'mass trace detection':";
                        }
                        else if (output.Contains("Progress of 'elution peak detection':"))
                        {
                            current_work = "Progress of 'elution peak detection':";
                        }
                        else if (output.Contains("Progress of 'assembling mass traces to features':"))
                        {
                            current_work = "Progress of 'assembling mass traces to features':";
                        }
                        else if (output.Contains("Progress of 'Aligning input maps':"))
                        {
                            current_work = "Progress of 'Aligning input maps':";
                        }
                        else if (output.Contains("Progress of 'linking features':"))
                        {
                            current_work = "Progress of 'linking features':";
                        }
                        else if (output.Contains("%"))
                        {
                            SendAndLogTemporaryMessage("{0} {1}", current_work, output);
                        }

                    }


                    // Note: The child process waits until everything is read from the standard output -> A Deadlock could arise here
                    using (var reader = new StringReader(process.StandardOutput.ReadToEnd()))
                    {
                        string output;

                        while ((output = reader.ReadLine()) != null)
                        {
                            WriteLogMessage(MessageLevel.Debug, output);

                            if (String.IsNullOrEmpty(output) == false)
                            {
                                SendAndLogMessage(output, false);
                            }
                        }
                    }

                    process.WaitForExit();
                }
                catch (InvalidOperationException ex)
                {
                    NodeLogger.ErrorFormat(ex, "The following exception is raised during the execution of \"{0}\":", execPath);
                    throw;
                }


                if (process.ExitCode != 0)
                {
                    throw new MagellanProcessingException(
                        String.Format(
                            "The exit code of {0} was {1}. (The expected exit code is 0)",
                            Path.GetFileName(process.StartInfo.FileName), process.ExitCode));
                }
            }
            catch (System.Threading.ThreadAbortException)
            {
                throw;
            }
            catch (Exception ex)
            {
                NodeLogger.ErrorFormat(ex, "The following exception is raised during the execution of \"{0}\":", execPath);
                throw;
            }
            finally
            {
                if (!process.HasExited)
                {
                    NodeLogger.WarnFormat(
                        "The process [{0}] isn't finished correctly -> force the process to exit now", process.StartInfo.FileName);
                    process.Kill();
                }
            }

            SendAndLogMessage("{0} tool processing took {1}.", execPath, StringHelper.GetDisplayString(timer.Elapsed));

        }
        #endregion

    }


}

