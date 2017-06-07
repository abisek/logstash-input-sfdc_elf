# Logstash Plugin for Salesforce's Event Log File

The Salesforce Event Log File [Ruby Gem](https://rubygems.org/gems/logstash-input-sfdc_elf/) plug-in simplifies the integration between 
the ELK stack and Event Log Files by allowing you to easily download and index Salesforce event data every day without a custom integration.

ELF on ELK on Docker is the quickest way to get started. Check the links below.

### Logstash Plugin Config
```
input {  
    sfdc_elf {
        # === Required Fields ===
        
        # Input the username and password of your Force.com organization.
        username       => ""
        password       => ""

        # Id & Secret are values that are generated when you create a "Connected App."
        client_id      => ""
        client_secret  => ""

        
        # === OPTIONAL FIELDS ===
        # Leave these fields commented out (with '#') if the config does not apply to you. Some of the fields 
        # have default values
        
        # Only needed when your Force.com organization requires it.
        # security_token => ""

        # The path to be use to store the .sfdc_info_logstash state persistor file. You set the path
        # like so, "~/SomeDirectory" Paths must be absolute and cannot be relative.
        # Defaults to your home directory "~/".
        # path           => ""

        # Specify how often the plugin should grab new data in terms of minutes.
        # Defaults to 1440 minutes (1 day). We recommend keeping it at 1 day.
        # poll_interval_in_minutes => 60
        
        # The host to use for OAuth2 authentication. 
        # Defaluts to "login.salesforce.com".
        # Use "test.salesforce.com" for connecting to Sandbox instance.
        # host           => ""

    }
}
```

### Setting up a Salesforce Connected App
Detailed instructions for setting up a Connected App can be found [here](https://help.salesforce.com/apex/HTViewHelpDoc?id=connected_app_create.htm).
When configuring the connected application, ensure the following options are configured:

1. *Enable OAuth Settings* is checked.
2. *Access and manage data (api)* and *Access your basic information (id, profile, email, address, phone)* are included in your *Selected OAuth Scopes*.

### Blogs, Articles, and Tutorials
1. [Elf on Elk on Docker](http://www.salesforcehacker.com/2015/10/elf-on-elk-on-docker.html) by Adam Torman
2. [Elf on Elk on Docker Image Source Code](https://github.com/developerforce/elf_elk_docker/blob/master/README.md)
3. ['Users: WE KNOW THEM' – The ELF@Salesforce](https://www.elastic.co/elasticon/conf/2016/sf/users-we-know-them-the-elf-at-salesforce) at Elastic{ON} '16 by Adam Torman and Abhishek Sreenivasa