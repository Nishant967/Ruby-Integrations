# Here I am taking adwords client configurations and other details from database to import campaign data from adwords for today.

class AdwordsDataFetch < ApplicationJob

     require 'adwords_api'
     require 'openssl'
     require 'logger'

      def self.get_api_version
        return :v201607
      end

      def self.perform
        begin
          result = {}
          adword_log='adword_log_'+Time.now.to_s+'.log'
          logfile_name = File.join(Rails.root, 'log', adword_log)
          # To log the entire process of adwords data collection
          logger = Logger.new(logfile_name)
          logger.level = Logger::DEBUG
          #Object which map to table in database having adwords credential is DspAdwordsConfiguration
          adword_configuration = AdwordsAccountDetails.all
          adword_configuration.each do |configuration|
              adwords = AdwordsApi::Api.new(
                                            {
                                                  :authentication =>
                                                  {
                                                    :method => configuration.method,
                                                    :oauth2_client_id => configuration.oauth2_client_id,
                                                    :oauth2_client_secret => configuration.oauth2_client_secret,
                                                    :developer_token => configuration.developer_token,
                                                    :user_agent => 'Get_Adwords_Report_For_Client',
                                                    :oauth2_token =>{
                                                                      :access_token => configuration.access_token,
                                                                      :refresh_token => configuration.refresh_token,
                                                                      :expires_in => configuration.expires_in,
                                                                      :issued_at => configuration.issued_at
                                                                    }
                                                  },
                                                  :service => {:environment => 'PRODUCTION'},
                                                  :connection => {:enable_gzip => false},
                                                  :library => {:log_level => 'DEBUG'}
                                            })
              adwords.logger = logger
              adwords.config.set("authentication.client_customer_id", configuration.client_customer_id)
              token = adwords.authorize() do |auth_url|
                puts "Auth error, please navigate to URL:\n\t%s" % auth_url
                puts 'log in and type the verification code:'
                verification_code = gets.chomp
              end

              if token
                hash = HashWithIndifferentAccess.new(token)
                referesh_token = hash['refresh_token']
                access_token = hash['access_token']
                developer_token = configuration.developer_token
                issued_at = hash['issued_at']
                expires_in = hash['expires_in']
                DspAdwordsConfiguration.where(:developer_token => developer_token)
                                       .update_all("access_token = '#{access_token}',
                                       refresh_token = '#{referesh_token}', expires_in = '#{expires_in}',
                                       issued_at = '#{issued_at}' ")
              else
                raise AdsCommon::Errors::Error, "Can't save nil token"
              end

              report_utils = adwords.report_utils(get_api_version())
              report_definition = {
                                    :selector => {
                                                  :fields => ['CampaignId','CampaignName',
                                                              'Clicks','Conversions','Impressions','Interactions',
                                                              'HourOfDay'
                                                             ],
                                                 },
                                    :report_name => 'TODAY CAMPAIGN PERFORMANCE REPORT',
                                    :report_type => 'CAMPAIGN_PERFORMANCE_REPORT',
                                    :download_format => 'XML',
                                    :date_range_type => 'TODAY',  # change date range according to your need
                                  }
             report_name='report_automation_'+Time.now.to_s+'_'+configuration.client_customer_id+'.xml'
             file_name = File.join(Rails.root, 'log', report_name)
             adwords.skip_report_header = false
             adwords.skip_column_header = false
             adwords.skip_report_summary = false
             adwords.include_zero_impressions = false
             report_utils.download_report_as_file(report_definition, file_name)
             xml = Nokogiri::XML(open(file_name))
                   xml.xpath('//row').each do |thing|
                      result[ReportImportConstants::ID] = thing.attribute('campaignID').value
                      result[ReportImportConstants::NAME] = thing.attribute('campaign').value
                      result[ReportImportConstants::CLICKS] = thing.attribute('clicks').value
                      result[ReportImportConstants::CONVERSIONS] = thing.attribute('conversions').value
                      result[ReportImportConstants::IMPRESSIONS] = thing.attribute('impressions').value
                      result[ReportImportConstants::HOUR] = thing.attribute('hourOfDay').value
                      result[ReportImportConstants::DATE] = xml.xpath('//date-range').attribute('date').value
                      reportdate = Date.parse result[ReportImportConstants::DATE]
                      result[ReportImportConstants::DATE] = reportdate
                      #here you already have data for the adwords accounts for campaigns now just write a method to store it wherever you want
                      upload(result)
                    end
          end

          rescue => e
            puts  "Exception issue #{e.message}"
          end

        end

end
