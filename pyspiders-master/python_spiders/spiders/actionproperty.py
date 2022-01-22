import re
import scrapy
from python_spiders.loaders import ListingLoader
from python_spiders.helper import remove_white_spaces, sq_feet_to_meters

class ActionpropertySpider(scrapy.Spider):
    name = 'actionproperty'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['actionproperty.ca']
    start_urls = ['https://actionproperty.ca/rental/']

    def parse(self, response):
        for item in response.css(".property_title a::attr(href)").getall():            
            yield scrapy.Request(url=item, callback=self.parse_page)
    
    def parse_page(self, response):
        title           = response.css(".property-title.entry-title::text").get()
        property_type   = check_prop(response.css(".property-title.entry-title::text").get())
        
        description     = re.sub("[Cc]all [Aa]ction [Pp]roperty.*","",response.css(".the_content p::text").get())
        
        if not property_type:
            property_type     = check_prop(response.css(".the_content p::text").get())
        
        available       = remove_white_spaces(response.css(".property_availability span.value::text").get())
        city            = response.css(".property_city span.value::text").get()
        address         = remove_white_spaces(response.css(".property_location span.value::text").get())
        rent            = int(response.css(".property_price span.value::text").get().strip().replace(",",""))
        deposit         = int(float(response.css(".property_deposit span.value::text").get().strip().replace(",","")))
        room_count      = int(float(response.css(".property_bedrooms span.value::text").get().replace(" + office","").strip().replace(",","")))
        bathroom_count  = int(float(response.css(".property_bathrooms span.value::text").get().strip().replace(",","")))
        utilities       = response.css(".property_additional_fees span.value::text").re("\$[0-9]+")
        images          = response.css(".sidebar_gallery_item a::attr(href)").getall()
        latitude, longitude = response.css("script").re('([0-9]+\.[0-9]+,\W*-[0-9]+.[0-9]+)')[0].split(",")
        pets_allowed = response.css(".property_pets span.value::text").get()

        if pets_allowed:
            pets_allowed = pets_allowed.strip().lower() == 'yes'

        if utilities:
            utilities =  int(float(utilities[0].replace("$","").replace(",","").strip()))
        
        if city:
            city = city.strip()
        
        landlord_name   = response.css(".phone span::text").get().replace(":","")
        
        landlord_phone  = response.css(".phone span::text").getall()[1].replace(".","-")
        external_id = response.css("link[rel='shortlink']::attr(href)").get().split("=")[1]
        # landlord_email  = response.css(".the_content p strong::text").re(r"Email:.*")
        
        # if landlord_email:
        #     landlord_email = landlord_email[0].replace("Email:").strip()
        
        item = ListingLoader(response=response)
        item.add_value("external_source"        ,self.external_source)
        item.add_value("address"                ,address)
        item.add_value("external_link"          ,response.url)
        item.add_value("currency"               ,"CAD")
        item.add_value("title"                  ,title)
        item.add_value("rent"                   ,rent)
        item.add_value("images"                 ,images)
        item.add_value("room_count"             ,room_count)
        item.add_value("bathroom_count"         ,bathroom_count)
        item.add_value("description"            ,description)
        item.add_value("landlord_phone"         ,landlord_phone)
        item.add_value("city"                   ,city)
        item.add_value("deposit"                ,deposit)
        item.add_value("landlord_name"          ,landlord_name)
        item.add_value("property_type"          ,property_type)
        item.add_value("longitude"              ,longitude)
        item.add_value("latitude"               ,latitude)
        item.add_value("landlord_email"         ,'reception@actionproperty.ca')
        item.add_value("pets_allowed"           ,pets_allowed)
        item.add_value("utilities"              ,utilities)

        
        if available == "Available":
            yield item.load_item()
        
        
def check_prop(val):
    val = val.lower()
    apartments = ['apartment']
    houses     = ['house', 'townhomes']
    for v in houses:
        if v in val:
            return 'house'
    for v in apartments:
        if v in val:
            return 'apartment'
