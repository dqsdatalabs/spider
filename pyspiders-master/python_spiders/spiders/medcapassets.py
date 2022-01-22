import scrapy
from python_spiders.loaders import ListingLoader
import re

class MedcapassetsSpider(scrapy.Spider):
    name = 'medcapassets'
    execution_type = 'testing'
    country = 'Canada'
    locale = 'Ca'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['medcapassets.com']
    start_urls = ['https://medcapassets.com/property-result/']

    def parse(self, response):
        for page in response.css(".property-list-style div a::attr(href)").getall():
            yield scrapy.Request(url=page, callback=self.parse_page)
            
    def parse_page(self, response):
        
        title           = response.css(".property-columns-left h1::text").get()
        rent            = response.css(".property-columns-left .price::text").get().strip().replace(",","").replace("$","")
        address         = response.css(".property-columns-left .format-address::text").get().strip()
        property_type   = make_prop(response.css(".property-columns-left .property_meta .type::text").get())
        room_count      = response.css(".property-columns-left .property-details-icons .bedrooms::text").get()
        bathroom_count  = response.css(".property-columns-left .property-details-icons .bathrooms::text").get()
        parking         = response.css(".property-columns-left ul .parking::text").get()
        images          = response.css("ul.slides li a::attr(href)").getall()
        description     = response.css(".summary .summary-contents ::text").get()
        
        desc = ""
        if len(description) == 0:
            description = response.css(".summary .summary-contents .panel.panel-default.stivaProductPanel div::text").getall()
            for i in description:
                desc+= i.strip()+" "
            description = desc.strip()
        
        description = make_desc(description)
        
        if rent != "-":
            rent = int(float(rent))
        
        swimming_pool         = make_fetures(response.css(".property-columns-left .features ul li::text").getall())
        
        
        
        item = ListingLoader(response=response)
        
        item.add_value("external_link"          ,response.url)
        item.add_value("external_source"        ,self.external_source)
        item.add_value("title"                  ,title)
        item.add_value("rent"                   ,rent)
        item.add_value("address"                ,address)
        item.add_value("property_type"          ,property_type)
        item.add_value("room_count"             ,room_count)
        item.add_value("bathroom_count"         ,bathroom_count)
        item.add_value("parking"                ,parking != "")
        item.add_value("images"                 ,images)
        item.add_value("description"            ,description)
        item.add_value("currency"               ,"USD")
        item.add_value("landlord_phone"         ,"647-495-6892")
        item.add_value("landlord_email"         ,"info@medcapassets.com")
        item.add_value("swimming_pool"          ,swimming_pool)
        
        
        if response.css(".availability::text").get().strip() == "For Rent":
            yield item.load_item()

def make_prop(prop):
    if not prop:
        return ""
    prop = prop.replace(":","").strip().lower()
    if prop in ['apt', 'condo']:
        return'apartment'
    return prop

def make_desc(desc):
    return re.sub("[Pp]lease* [Cc]ontact.*","",desc)


def make_fetures(f):
    for i in f:
        if 'pool' in i :
            return True
