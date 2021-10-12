#from data_ingestion import data_loader
#from DataTransform import DataTransformation
from application_logging import logger
import json
import os
import pandas as pd


class dataTransform:
    """
              This class shall be used for transforming the Data!.

              Written By: Mrinal Srivastava
              Version: 1.0
              Revisions: None

              """

    def __init__(self, file_object, logger_object, df_app, df_session, df_transcripts):
        self.outputfolder1 = "df_app_id_cnt.csv"
        self.outputfolder2 = "df_app_session_cnt.csv"
        self.outputfolder3 = "df_app_user_cnt.csv"
        self.file_object = file_object
        self.logger_object = logger_object
        self.df_app = df_app
        self.df_session = df_session
        self.df_transcripts = df_transcripts

    def changecolumnheader(self):
        """
           Method Name: changecolumnheader
           Description: change Columns header
           Written By: Mrinal Srivastava
           Version: 1.0
           Revisions: None

           """

        try:
            def datetime_conv(dt):
                return pd.to_datetime(dt, unit='ns')

            self.logger_object.log(self.file_object, 'Data Transformation changecolumnheader started')

            self.df_session = self.df_session.rename(
                columns={'createdAt.$date': 'createddate', '_id.$oid': 'idoid', 'app.$oid': 'appoid'})
            self.df_transcripts = self.df_transcripts.rename(
                columns={'_id.$oid': '_idoid_transcript', 'session.$oid': 'sessionoid_transcript',
                         'createdAt.$date': 'createdAtdate_transcript'})

            self.df_session['createddatemp'] = self.df_session['createddate'].map(datetime_conv)
            self.df_transcripts['cdate_transcript'] = self.df_transcripts['createdAtdate_transcript'].map(datetime_conv)

            self.logger_object.log(self.file_object, "Data Transformation to changecolumnheader end ")

        except Exception as e:
            self.logger_object.log(self.file_object, "Data Transformation failed because:: %s" % e)

    def dataprocessing(self):
        """
                     Method Name: dataprocessing
                     Description: dataprocessing

                      Written By: Mrinal Srivastava
                     Version: 1.0
                     Revisions: None

                                                 """

        try:

            self.logger_object.log(self.file_object, 'Data Transformation dataprocessing started')
            merged_inner = self.df_app.merge(self.df_session, how='inner', left_on='_id.$oid', right_on='appoid')
            merged_all = merged_inner.merge(self.df_transcripts, how='inner', left_on='idoid',
                                            right_on='sessionoid_transcript')
            self.logger_object.log(self.file_object, 'Data Transformation dataprocessing Merging completed')
            # Number of sessions of a chatbot application (appId)
            self.logger_object.log(self.file_object, 'Data Transformation dataprocessing Merging completed')

            ## App_id + session Join  Number of sessions of a chatbot application (appId)
            # . Number of sessions of a chatbot application (appId)

            print (self.outputfolder1)
            self.logger_object.log(self.file_object, 'Number of sessions of a chatbot application (appId) generated')
            df_app_id_cnt = merged_inner.groupby(['appId']).appId.agg(['count'])
            df_app_id_cnt.to_csv(self.outputfolder1)
            print('shape', df_app_id_cnt.shape)
            ### 2. Average session duration of a chatbot application
            self.logger_object.log(self.file_object, '2. Average session duration of a chatbot application')
            df_app_session_cnt = merged_all.groupby('appId', as_index=False)['createdAtdate_transcript'].mean()
            df_app_session_cnt.to_csv(self.outputfolder2)

            # Average number of user queries per session of a chatbot application
            self.logger_object.log(self.file_object,
                                   'Average number of user queries per session of a chatbot application')
            df_user2 = merged_all.groupby(['appId', 'sessionoid_transcript'], as_index=False)['from'].count()
            df_app_user_cnt = df_user2.groupby(['appId'], as_index=False)['from'].mean()
            df_app_user_cnt.to_csv(self.outputfolder3)

            self.logger_object.log(self.file_object, "dataprocessing End ")

        except Exception as e:
            self.logger_object.log(self.file_object, "Data dataprocessing failed because:: %s" % e)


class Data_Getter:
    """
    This class shall  be used for obtaining the data from the source for Pipeline.

    Written By: Mrinal Srivastava
    Version: 1.0
    Revisions: None

    """
    def __init__(self, file_object, logger_object):
        self.data_file1='apps.json'
        self.data_file2 = 'sessions.json'
        self.data_file3 = 'transcripts.json'
        self.file_object=file_object
        self.logger_object=logger_object

    def get_data(self):
        """
        Method Name: get_data
        Description: This method reads the data from source.
        Output: A pandas DataFrame.
        On Failure: Raise Exception

         Written By: Mrinal Srivastava
        Version: 1.0
        Revisions: None

        """
        self.logger_object.log(self.file_object,'Entered the get_data method of the Data_Getter class')
        try:
            f = open(self.data_file1, "r")
            app_file = json.loads(f.read())
            df_app = pd.json_normalize(app_file)
            f.close()

            f = open(self.data_file2, "r")
            session_file = json.loads(f.read())
            df_session = pd.json_normalize(session_file)
            f.close()

            f = open(self.data_file3, "r")
            transcripts_file = json.loads(f.read())
            df_transcripts = pd.json_normalize(transcripts_file)
            f.close()


            self.logger_object.log(self.file_object,'Data Load Successful.Exited the get_data method of the Data_Getter class')
            return df_app,df_session,df_transcripts

        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in get_data method of the Data_Getter class. Exception message: '+str(e))
            self.logger_object.log(self.file_object,
                                   'Data Load Unsuccessful.Exited the get_data method of the Data_Getter class')
            raise Exception()




def dataloadder():
    log_writer = logger.App_Logger()
    log_file = "loaderlogs.txt"
    file_object = open(log_file, 'a+')
    log_writer.log(file_object, 'Start of loading Json data')
    try:
        # Getting the data from the source
        data_getter=Data_Getter(file_object,log_writer)
        df_app, df_session,df_transcripts =data_getter.get_data()
        print(df_app.shape, df_session.shape,df_transcripts.shape)
        log_writer.log(file_object, 'loading Json data completed for df_app' + str(df_app.shape))
        log_writer.log(file_object, 'loading Json data completed for df_session' + str(df_app.shape))
        log_writer.log(file_object, 'loading Json data completed for df_session' + str(df_app.shape))
        log_writer.log(file_object, 'loading Json data completed')
        return df_app, df_session, df_transcripts

    except ValueError:
            return Response("Error Occurred! %s" %ValueError)
    except KeyError:
        return Response("Error Occurred! %s" %KeyError)
    except Exception as e:
        return Response("Error Occurred! %s" %e)

def transformation(df_app, df_session, df_transcripts):
    log_writer = logger.App_Logger()
    file_object = open("transformation.txt", 'a+')
    log_writer.log(file_object, 'transformation data started')
    try:
        data_transformation = dataTransform(file_object, log_writer,df_app, df_session, df_transcripts)
        data_transformation.changecolumnheader()
        data_transformation.dataprocessing()
        print(df_app.shape, df_session.shape,df_transcripts.shape)

    except ValueError:
            return Response("Error Occurred! %s" %ValueError)
    except KeyError:
        return Response("Error Occurred! %s" %KeyError)
    except Exception as e:
        return Response("Error Occurred! %s" %e)


if __name__ == "__main__":
    df_app, df_session, df_transcripts = dataloadder()
    transformation(df_app, df_session, df_transcripts)