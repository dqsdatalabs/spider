import scrapy
from python_spiders.loaders import ListingLoader
import re


class RpmserviceSpider(scrapy.Spider):
    name = 'rpmservice'
    execution_type = 'development'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    
    allowed_domains = ['rpmservice.ca']
    start_urls = ['https://rpmservice.ca/properties-for-rent-gta-toronto/']

    def parse(self, response):
        for page in set(response.css(".wp3d-models-clearfix.filtering-on.map-only-off div a::attr(href)").getall()):
            yield scrapy.Request(page, callback=self.parse_page)
    
    def parse_page(self, response):
        images          = response.css(".wp3d-embed-wrap .lazy::attr(data-bg)").getall()
        rent            = make_rent(response.xpath('//ul/li[contains(text(), "Discounted Rent | Legal Rent")]/em/text()').get()) 
        room_count      = make_room(response.xpath('//ul/li[contains(text(), "Beds ")]/em/text()').get())
        bathroom_count  = make_paths(response.xpath('//ul/li[contains(text(), "Baths ")]/em/text()').get())
        square_meters   = make_square(response.xpath('//ul/li[contains(text(), "Size ")]/em/text()').get())
        parking         = response.xpath('//ul/li[contains(text(), "Parking ")]/em/text()').get()
        dishwasher      = response.xpath('//ul/li[contains(text(), "Dishwasher ")]/em/text()').get()
        floor           = response.xpath('//ul/li[contains(text(), "Floor/Level ")]/em/text()').get()
        description     = " ".join(response.css('.entry-content.wp3d-entry-content p::text').getall())
        address         = response.css('.entry-header.wp3d-entry-header h1::text').get()
        title           = response.css('.entry-header.wp3d-entry-header h2::text').get()
        property_type   = make_prop(response.css('.entry-header.wp3d-entry-header h2::text').get())
        dogs            = response.xpath('//ul/li[contains(text(), "Dogs allowed ")]/em/text()').get()
        cats            = response.xpath('//ul/li[contains(text(), "Cats allowed ")]/em/text()').get()
        available_date  = response.xpath('//ul/li[contains(text(), "Available ")]/em/text()').get()
        not_availble    = response.css('h1 span#Property_no_longer_avaiable').get()
        
        item = ListingLoader(response=response)
        item.add_value("external_source"        ,self.external_source)
        item.add_value("external_link"          ,response.url)
        item.add_value("title"                  ,title)
        item.add_value("address"                ,address)
        item.add_value("rent"                   ,rent)
        item.add_value("images"                 ,images)
        item.add_value("property_type"          ,property_type)
        item.add_value("room_count"             ,room_count)
        item.add_value("bathroom_count"         ,bathroom_count)
        item.add_value("square_meters"          ,int(int(square_meters)*10.764))
        item.add_value("parking"                ,parking != "No")
        item.add_value("description"            ,description)
        item.add_value("currency"               ,"USD")
        item.add_value("landlord_email"         ,'service.leasing@realpropertymgt.ca')
        item.add_value("landlord_phone"         ,'(416) 642-1404')
        item.add_value("landlord_name"          ,'Jerome Schrier')
        
        item.add_value("dishwasher"             ,dishwasher!="No")
        item.add_value("pets_allowed"           ,dogs=='Yes' or cats=='Yes')
        item.add_value("floor"                  ,floor)
        
        if available_date != "30 days or sooner" and not not_availble:
            yield item.load_item()



def make_rent(rent):
    if not rent:
        return rent
    if rent.find("|") > -1:
        return round(float(rent[:rent.find("|")].replace("$","").replace(",","").strip()))

def make_room(val):
    if val == "Studio":
        return 1
    try:
        return int(val)
    except :
        return ""


def make_prop(prop):
    apartments = ['Bungalow', 'Basement', 'Condo','Suite', 'Home' ,'Suburban', 'Stunning']
    houses     = ["Townhome"]
    types = houses+apartments
    if prop is None:
        return ""
    for i in types:
        if i in prop:
            if i in apartments:
                return 'apartment'
            if i in houses:
                return 'house'
    return prop

def make_paths(val):
    if val is not None:
        return int(float(val))
    return val


def make_square(sq):    
    if sq is None:
        return ""
    return round(int(re.findall("[0-9]+",sq)[0])*0.0929)
