/*
 * Copyright (c) 2002-2008, Mairie de Paris
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *  1. Redistributions of source code must retain the above copyright notice
 *     and the following disclaimer.
 *
 *  2. Redistributions in binary form must reproduce the above copyright notice
 *     and the following disclaimer in the documentation and/or other materials
 *     provided with the distribution.
 *
 *  3. Neither the name of 'Mairie de Paris' nor 'Lutece' nor the names of its
 *     contributors may be used to endorse or promote products derived from
 *     this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 * License 1.0
 */
package fr.paris.lutece.plugins.directory.modules.solr.search;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.lucene.demo.html.HTMLParser;
import fr.paris.lutece.plugins.directory.business.Directory;
import fr.paris.lutece.plugins.directory.business.DirectoryFilter;
import fr.paris.lutece.plugins.directory.business.DirectoryHome;
import fr.paris.lutece.plugins.directory.business.EntryFilter;
import fr.paris.lutece.plugins.directory.business.EntryHome;
import fr.paris.lutece.plugins.directory.business.IEntry;
import fr.paris.lutece.plugins.directory.business.Record;
import fr.paris.lutece.plugins.directory.business.RecordField;
import fr.paris.lutece.plugins.directory.business.RecordFieldFilter;
import fr.paris.lutece.plugins.directory.business.RecordFieldHome;
import fr.paris.lutece.plugins.directory.business.RecordHome;
import fr.paris.lutece.plugins.directory.service.DirectoryPlugin;
import fr.paris.lutece.plugins.search.solr.business.field.Field;
import fr.paris.lutece.plugins.search.solr.indexer.SolrIndexer;
import fr.paris.lutece.plugins.search.solr.indexer.SolrItem;
import fr.paris.lutece.portal.service.content.XPageAppService;
import fr.paris.lutece.portal.service.message.SiteMessageException;
import fr.paris.lutece.portal.service.plugin.Plugin;
import fr.paris.lutece.portal.service.plugin.PluginService;
import fr.paris.lutece.portal.service.util.AppLogService;
import fr.paris.lutece.portal.service.util.AppPathService;
import fr.paris.lutece.portal.service.util.AppPropertiesService;
import fr.paris.lutece.util.url.UrlItem;


/**
 * The Directory indexer for Solr search platform
 *
 */
public class SolrDirectoryIndexer implements SolrIndexer
{
    private static final String PROPERTY_DESCRIPTION = "directory-solr.indexer.description";
    private static final String PROPERTY_NAME = "directory-solr.indexer.name";
    private static final String PROPERTY_VERSION = "directory-solr.indexer.version";
    private static final String PROPERTY_INDEXER_ENABLE = "directory-solr.indexer.enable";

    // Site name
    private static final String PROPERTY_SITE = "lutece.name";
    private static final String PROPERTY_PROD_URL = "lutece.prod.url";
    public static final String SHORT_NAME = "dry";
    private static final String DIRECTORY = "directory";
    private static final String PARAMETER_ID_DIRECTORY_RECORD = "id_directory_record";
    private static final String PARAMETER_VIEW_DIRECTORY_RECORD = "view_directory_record";
    private static final String ROLE_NONE = "none";
    private String _strSite;
    private String _strProdUrl;

    public SolrDirectoryIndexer(  )
    {
        super(  );
        _strSite = AppPropertiesService.getProperty( PROPERTY_SITE );
        _strProdUrl = AppPropertiesService.getProperty( PROPERTY_PROD_URL );

        if ( !_strProdUrl.endsWith( "/" ) )
        {
            _strProdUrl = _strProdUrl + "/";
        }
    }

    public String getDescription(  )
    {
        return AppPropertiesService.getProperty( PROPERTY_DESCRIPTION );
    }

    public String getName(  )
    {
        return AppPropertiesService.getProperty( PROPERTY_NAME );
    }

    public String getVersion(  )
    {
        return AppPropertiesService.getProperty( PROPERTY_VERSION );
    }

    public Map<String, SolrItem> index(  )
    {
        Map<String, SolrItem> items = new HashMap<String, SolrItem>(  );
        Plugin plugin = PluginService.getPlugin( DirectoryPlugin.PLUGIN_NAME );

        // Index only the directories that have the attribute is_indexed as true
        DirectoryFilter dirFilter = new DirectoryFilter(  );
        dirFilter.setIsIndexed( DirectoryFilter.FILTER_TRUE );
        dirFilter.setIsDisabled( DirectoryFilter.FILTER_TRUE ); //Bad naming: IsDisable( true ) stands for enabled

        for ( Directory directory : DirectoryHome.getDirectoryList( dirFilter, plugin ) )
        {
            int nIdDirectory = directory.getIdDirectory(  );

            //Index only the records that have the attribute is_enable as true
            RecordFieldFilter recFilter = new RecordFieldFilter(  );
            recFilter.setIdDirectory( nIdDirectory );
            recFilter.setIsDisabled( RecordFieldFilter.FILTER_TRUE ); //Bad naming: IsDisable( true ) stands for enabled

            List<Record> listRecord = RecordHome.getListRecord( recFilter, plugin );

            //Keep processing this directory only if there are enabled records
            if ( !listRecord.isEmpty(  ) )
            {
                //Parse the entries to gather the ones marked as indexed
                EntryFilter entryFilter = new EntryFilter(  );
                entryFilter.setIdDirectory( nIdDirectory );
                entryFilter.setIsIndexed( EntryFilter.FILTER_TRUE );

                List<IEntry> listIndexedEntry = EntryHome.getEntryList( entryFilter, plugin );

                entryFilter.setIsIndexed( EntryFilter.ALL_INT );
                entryFilter.setIsIndexedAsTitle( EntryFilter.FILTER_TRUE );

                List<IEntry> listIndexedAsTitleEntry = EntryHome.getEntryList( entryFilter, plugin );

                entryFilter.setIsIndexedAsTitle( EntryFilter.ALL_INT );
                entryFilter.setIsIndexedAsSummary( EntryFilter.FILTER_TRUE );

                List<IEntry> listIndexedAsSummaryEntry = EntryHome.getEntryList( entryFilter, plugin );

                for ( Record record : listRecord )
                {
                    try
                    {
                        SolrItem recordDoc = getDocument( record, listIndexedEntry, listIndexedAsTitleEntry,
                                listIndexedAsSummaryEntry, plugin );

                        if ( recordDoc != null )
                        {
                            items.put( getLog( recordDoc ), recordDoc );
                        }
                    }
                    catch ( IOException e )
                    {
                        AppLogService.error( e );
                    }
                }
            }
        }

        return items;
    }

    public boolean isEnable(  )
    {
        return "true".equalsIgnoreCase( AppPropertiesService.getProperty( PROPERTY_INDEXER_ENABLE ) );
    }

    public List<Field> getAdditionalFields(  )
    {
        return new ArrayList<Field>(  );
    }

    /**
     * {@inheritDoc}
     */
    public List<SolrItem> getDocuments( String recordId )
        throws IOException, InterruptedException, SiteMessageException
    {
        Plugin plugin = PluginService.getPlugin( DirectoryPlugin.PLUGIN_NAME );

        int nIdRecord;

        try
        {
            nIdRecord = Integer.parseInt( recordId );
        }
        catch ( NumberFormatException ne )
        {
            AppLogService.error( recordId + " not parseable to an int", ne );

            return new ArrayList<SolrItem>( 0 );
        }

        Record record = RecordHome.findByPrimaryKey( nIdRecord, plugin );
        Directory directory = record.getDirectory(  );

        if ( !record.isEnabled(  ) || !directory.isEnabled(  ) || !directory.isIndexed(  ) )
        {
            return new ArrayList<SolrItem>( 0 );
        }

        int nIdDirectory = directory.getIdDirectory(  );

        //Parse the entries to gather the ones marked as indexed
        EntryFilter entryFilter = new EntryFilter(  );
        entryFilter.setIdDirectory( nIdDirectory );
        entryFilter.setIsIndexed( EntryFilter.FILTER_TRUE );

        List<IEntry> listIndexedEntry = EntryHome.getEntryList( entryFilter, plugin );

        entryFilter.setIsIndexed( EntryFilter.ALL_INT );
        entryFilter.setIsIndexedAsTitle( EntryFilter.FILTER_TRUE );

        List<IEntry> listIndexedAsTitleEntry = EntryHome.getEntryList( entryFilter, plugin );

        entryFilter.setIsIndexedAsTitle( EntryFilter.ALL_INT );
        entryFilter.setIsIndexedAsSummary( EntryFilter.FILTER_TRUE );

        List<IEntry> listIndexedAsSummaryEntry = EntryHome.getEntryList( entryFilter, plugin );

        SolrItem doc = getDocument( record, listIndexedEntry, listIndexedAsTitleEntry, listIndexedAsSummaryEntry, plugin );

        if ( doc != null )
        {
            List<SolrItem> listDocument = new ArrayList<SolrItem>( 1 );
            listDocument.add( doc );

            return listDocument;
        }
        else
        {
            return new ArrayList<SolrItem>( 0 );
        }
    }

    /**
     * Builds a {@link SolrItem} which will be used by Solr during the indexing of this record
     * @param record the record to convert into a document
     * @param listContentEntry the entries in this record that are marked as is_indexed
     * @param listTitleEntry the entries in this record that are marked as is_indexed_as_title
     * @param listSummaryEntry the entries in this record that are marked as is_indexed_as_summary
     * @param plugin the plugin object
     * @return a Solr item filled with the record data
     * @throws IOException
     */
    private SolrItem getDocument( Record record, List<IEntry> listContentEntry, List<IEntry> listTitleEntry,
        List<IEntry> listSummaryEntry, Plugin plugin )
        throws IOException
    {
        SolrItem item = new SolrItem(  );

        boolean bFallback = false;

        //Fallback if there is no entry marker as indexed_as_title
        //Uses the first indexed field instead
        if ( listTitleEntry.isEmpty(  ) && !listContentEntry.isEmpty(  ) )
        {
            listTitleEntry.add( listContentEntry.get( 0 ) );
            bFallback = true;
        }

        String strTitle = getContentToIndex( record, listTitleEntry, plugin );

        //Fallback if fields were empty
        //Uses the first indexed field instead
        if ( StringUtils.isBlank( strTitle ) && !bFallback && !listContentEntry.isEmpty(  ) )
        {
            listTitleEntry.clear(  );
            listTitleEntry.add( listContentEntry.get( 0 ) );
            strTitle = getContentToIndex( record, listTitleEntry, plugin );
        }

        //No more fallback. Giving up
        if ( StringUtils.isBlank( strTitle ) )
        {
            return null;
        }

        // Setting the Title field
        item.setTitle( strTitle );

        if ( !listContentEntry.isEmpty(  ) )
        {
            String strContent = getContentToIndex( record, listContentEntry, plugin );

            if ( StringUtils.isNotBlank( strContent ) )
            {
                // Setting the Content field
                StringReader readerPage = new StringReader( strContent );
                HTMLParser parser = new HTMLParser( readerPage );

                Reader reader = parser.getReader(  );
                int c;
                StringBuffer sb = new StringBuffer(  );

                while ( ( c = reader.read(  ) ) != -1 )
                {
                    sb.append( String.valueOf( (char) c ) );
                }

                reader.close(  );
                item.setContent( sb.toString(  ) );
            }
        }

        if ( !listSummaryEntry.isEmpty(  ) )
        {
            String strSummary = getContentToIndex( record, listSummaryEntry, plugin );

            if ( StringUtils.isNotBlank( strSummary ) )
            {
                // Setting the Summary field
                item.setSummary( strSummary );
            }
        }

        String strRoleKey = record.getRoleKey(  );

        if ( StringUtils.isBlank( strRoleKey ) )
        {
            strRoleKey = ROLE_NONE;
        }

        // Setting the role field
        item.setRole( strRoleKey );

        // Setting the date field
        item.setDate( record.getDateCreation(  ) );

        UrlItem url = new UrlItem( _strProdUrl + AppPathService.getPortalUrl(  ) );
        url.addParameter( XPageAppService.PARAM_XPAGE_APP, DIRECTORY );
        url.addParameter( PARAMETER_ID_DIRECTORY_RECORD, record.getIdRecord(  ) );
        url.addParameter( PARAMETER_VIEW_DIRECTORY_RECORD, "" );
        // Setting the Url field
        item.setUrl( url.getUrl(  ) );

        //Add the uid as a field, so that index can be incrementally maintained.
        // This field is not stored with question/answer, it is indexed, but it is not
        // tokenized prior to indexing.
        String strUID = Integer.toString( record.getIdRecord(  ) ) + "_" + SHORT_NAME;
        // Setting the Uid field
        item.setUid( strUID );

        // Setting the Type field
        item.setType( DIRECTORY );

        // Setting the Site field
        item.setSite( _strSite );

        return item;
    }

    /**
     * Concatenates the value of the specified field in this record
     * @param record the record to seek
     * @param listEntry the list of field to concatenate
     * @param plugin the plugin object
     * @return
     */
    private String getContentToIndex( Record record, List<IEntry> listEntry, Plugin plugin )
    {
        List<Integer> listIdEntry = new ArrayList<Integer>( listEntry.size(  ) );

        for ( IEntry entry : listEntry )
        {
            listIdEntry.add( entry.getIdEntry(  ) );
        }

        StringBuffer sb = new StringBuffer(  );

        List<RecordField> listField = RecordFieldHome.getRecordFieldSpecificList( listIdEntry, record.getIdRecord(  ),
                plugin );

        for ( RecordField field : listField )
        {
            sb.append( RecordFieldHome.findByPrimaryKey( field.getIdRecordField(  ), plugin ).getValue(  ) );
            sb.append( " " );
        }

        return sb.toString(  );
    }

    /**
     * Generate the log line for the specified {@link SolrItem}
     * @param item The {@link SolrItem}
     * @return The string representing the log line
     */
    private String getLog( SolrItem item )
    {
        StringBuilder sbLogs = new StringBuilder(  );
        sbLogs.append( "indexing " );
        sbLogs.append( item.getType(  ) );
        sbLogs.append( " id : " );
        sbLogs.append( item.getUid(  ) );
        sbLogs.append( " Title : " );
        sbLogs.append( item.getTitle(  ) );
        sbLogs.append( "<br/>" );

        return sbLogs.toString(  );
    }
}
