from typing import Iterator
import os

import io
from sqlalchemy import create_engine
import pysftp
import pandas as pd

from mis.connectors.base.connector import FileMixin, BaseConnector, DataframeToSqlSaveMixin
from mis.connectors.base.models import BaseModel
from mis.connectors.fk_sftp.models import FkSftp, DB_SCHEMA
from mis.data.importers.file import CsvFileImporter 
from mis.data.fetchers.file import DiskFileFetcher
from mis.utils import logging
from mis.filters.markets import map_country_to_market, UNCATEGORIZED
import mis.filters.markets

logger = logging.get_logger(__name__)
FKSFTP_COL_MAPPING = FkSftp.get_renaming_map()

class FkSftpConnector(DataframeToSqlSaveMixin, FileMixin, BaseConnector):
    
    model_class = FkSftp
    home_dir = os.environ.get('HOME')
    priv_key_path = os.path.join(home_dir, '.ssh', 'id_rsa')

    def __init__(
            # SFTP server default connection parameters if not provided.
            self,
            sftphost = '192.168.2.1',
            sftpuser = 'root',
            sftpkey = priv_key_path,
            sftpport = 232,
            sftppassword = 'admin',
            sftppath = '/mnt/sftp',
            # Postgres default connection parameters if not provided.
            pgschema = DB_SCHEMA,
            pgtablename = FkSftp.__tablename__,
            pghost = 'localhost',
            pgport = '5432',
            pguser = 'postgres',
            pgpass = '417587a',
            pgdatabase = 'mis'
        ):
        
        super().__init__()
        self.file_importer = CsvFileImporter()
        self.sftphost = sftphost
        self.sftpuser = sftpuser
        self.sftpkey = sftpkey
        self.sftpport = sftpport
        self.sftppassword = sftppassword
        self.sftppath = sftppath
        self.pgschema = pgschema
        self.pghost = pghost
        self.pgport = pgport
        self.pguser = pguser
        self.pgpass = pgpass
        self.pgdatabase = pgdatabase
        self.table_name = pgtablename

    def data_to_instances(self) -> Iterator[BaseModel]:
        return self.data.iterrows()

    def get_raw_data(self):
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        try:
            # Connect to the SFTP server and download the file
            with pysftp.Connection(self.sftphost, username=self.sftpuser, private_key=self.sftpkey, port=self.sftpport, cnopts=cnopts) as sftp:
            # Create a df to concatenate all csv into one df
                all_data_frame = pd.DataFrame()
                # Iterate through the files in the remote directory
                for filename in sftp.listdir(self.sftppath):
                    # Check if the file end with .csv
                    if not sftp.isfile(f'{self.sftppath}/{filename}') or not filename.endswith('.csv'):
                        continue
                    # Download
                    logger.debug(filename) 
                    sftp.get(f'{self.sftppath}/{filename}')
                    # Convert the file to a pandas DataFrame
                    df = self.file_importer.import_data(open(filename))
                    # Concatenate all files found on sftp
                    all_data_frame = pd.concat([all_data_frame,df])
                    # Remove converted file
                    #sftp.remove(f'{self.sftppath}/{filename}')
                # Upload final DataFrame to postgres
                logger.debug(all_data_frame)
                self._topostgres(all_data_frame)
                self.data = all_data_frame
                # Return the Dataframe for another use.
                return all_data_frame
        except pysftp.ConnectionException as e:
            logger.error(f"Failed to connect to SFTP server: {str(e)}")
        except pysftp.AuthenticationException as e:
            logger.error(f"Authentication failed for SFTP server: {str(e)}")
        except Exception as e:
            logger.error(f"An error occurred during SFTP operation: {str(e)}")

    def _topostgres(self, all_data_frame):
        # Create the engine conection to postgres
        engine = create_engine(f'postgresql+psycopg2://pppostgres:417587a@localhost:5432/mis')
        
        # Send DataFrame to sql
        all_data_frame.to_sql(self.table_name, engine, if_exists='append', schema=self.pgschema, index_label='id')


    def apply_filter6_markets(self) -> None:

        def _get_market(country_code_: str) -> str:
            try:
                return mis.filters.markets.map_country_to_market(country_code_)
            except KeyError:
                return mis.filters.markets.UNCATEGORIZED

        self.data['st_market'] = self.data['isocountrycode'].apply(
            _get_market)


def main():
    connector = FkSftpConnector()
    connector.execute()


if __name__ == '__main__':
    main()