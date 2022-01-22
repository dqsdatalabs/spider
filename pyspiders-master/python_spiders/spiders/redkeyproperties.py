import scrapy
from python_spiders.loaders import ListingLoader
import re
from python_spiders.helper import remove_white_spaces

class RedkeypropertiesSpider(scrapy.Spider):
    name = 'redkeyproperties'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    
    allowed_domains = ['redkeyproperties.ca']
    start_urls = ['https://www.redkeyproperties.ca/for-rent/?wplorderby=p.mls_id&wplorder=DESC&wplpage=1&limit=100']

    def parse(self, response):
        for page in response.css(".wpl_prp_top_boxes .view_detail ::attr(href)").getall():
            yield scrapy.Request(url=page, callback=self.parse_page)
    
    
    def parse_page(self, response):
        
        images          = response.css("ul.wpl-gallery-pshow li::attr(data-src)").getall()
        address         = response.css("h2.location_build_up .wpl-location::text").get()
        title           = response.xpath('//div[contains(text(), "Property Title : ")]/span').css("::text").get()
        property_type   = make_prop(response.xpath('//div[contains(text(), "Property Type : ")]/span').css("::text").get())
        external_id     = response.xpath('//div[contains(text(), "Listing ID : ")]/span').css("::text").get()
        rent            = response.xpath('//div[contains(text(), "Price : ")]/span').css("::text").get()
        room_count      = response.xpath('//div[contains(text(), "Bedrooms : ")]/span').css("::text").get()
        bathroom_count  = response.xpath('//div[contains(text(), "Bathrooms : ")]/span').css("::text").get()
        city            = response.css(".wpl-column.rows.location.City span::text").get()
        floor           = response.xpath('//div[contains(text(), "Floor Number : ")]/span').css("::text").get()
        longitude       = response.xpath('//div[contains(text(), "Longitude : ")]/span').css("::text").get()
        latitude        = response.xpath('//div[contains(text(), "Latitude : ")]/span').css("::text").get()
        zipcode         = response.xpath('//div[contains(text(), "Postal Code")]/span').css("::text").get()
        floor           = response.xpath('//div[contains(text(), "Floor Number")]/span').css("::text").get()
        
        
        dishwasher, pets_allowed, parking, balcony, washing_machine = fetures(response.css(".wpl-column.rows.feature.single::text").getall())
        description     = remove_white_spaces(make_desc(" ".join(response.css("div.wpl_prp_show_detail_boxes_cont div div .et_builder_inner_content.et_pb_gutters3 p::text").getall())))
        list_type       = response.xpath('//div[contains(text(), "Listing Type : ")]/span').css("::text").get()
        
        
        if list_type == "For Rent" and property_type != "parking stall" and room_count != '0':
            
            item = ListingLoader(response=response)
            item.add_value("external_source"            ,self.external_source)
            # item.add_value("landlord_email"             ,"info@redkeyproperties.ca")
            item.add_value("landlord_phone"             ,"1-403-340-0065")
            item.add_value("currency"                   ,"CAD")
            item.add_value("images"                     ,images)
            item.add_value("address"                    ,address) 
            item.add_value("description"                ,description)
            item.add_value("title"                      ,title)
            item.add_value("property_type"              ,property_type)
            item.add_value("external_id"                ,external_id)
            item.add_value("external_link"              ,response.url)
            item.add_value("rent"                       ,int(rent.replace("C$","").strip()))
            item.add_value("room_count"                 ,int(float(room_count)))
            item.add_value("bathroom_count"             ,int(float(bathroom_count)))
            item.add_value("city"                       ,city)
            item.add_value("floor"                      ,floor)
            item.add_value("longitude"                  ,longitude)
            item.add_value("latitude"                   ,latitude)
            item.add_value("dishwasher"                 ,dishwasher)
            item.add_value("pets_allowed"               ,pets_allowed)
            item.add_value("parking"                    ,parking)
            item.add_value("balcony"                    ,balcony)
            item.add_value("washing_machine"            ,washing_machine)
            item.add_value("zipcode"                    ,zipcode)
        
            yield item.load_item()

def fetures(l):
    dish    = ""
    pets    = ""
    parking = ""
    balcony = ""
    washing_machine = ""
    if "Dishwasher" in l:
        dish = True
        
    if "Pet Friendly" in l:
       pets = True
    
    if "No Pets" in l:
        pets = False
    
    if "Balcony" in l:
        balcony = True 
    
    for i in l:
        if "parking" in i.lower():
            parking = True
        elif "laundry" in i.lower() or 'washer'  in i.lower():
            washing_machine = True
        
    return dish, pets, parking, balcony, washing_machine


def make_desc(desc):
    return re.sub("[Pp]lease [Cc][Aa][Ll][Ll] Red Key.*","",re.sub("[Ii]f you are.*","",desc))


def make_prop(prop):
    if prop:
        prop = prop.lower()
        if prop in ['condo', '1/2 duplex', '4-plex', '8-plex','10-plex', 'basement', 'main floor','6-plex']:
            return 'apartment'
        elif prop in ['townhouse']:
            return 'house'
    return prop
