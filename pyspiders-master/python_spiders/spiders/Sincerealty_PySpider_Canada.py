import scrapy
from python_spiders.loaders import ListingLoader
from datetime import datetime
import dateutil.parser
import re


class SincerealtySpider(scrapy.Spider):
    name = 'sincerealty'
    execution_type = 'testing'
    country = 'Canada'
    locale = 'Ca'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['sincerealty.com']
    start_urls = ['https://sincerealty.com/en/offers/for-rent/']

    def parse(self, response):
        for item in response.css("div.property-item-wrap div.property-header a::attr(href)").getall():
            yield scrapy.Request(url=item, callback=self.parse_page)
        next_page = response.css("div.noo-pagination.text-right a::attr(href)").get()
        if next_page:
            yield scrapy.Request(url=next_page, callback=self.parse)
            
    def parse_page(self, response):
        title           = response.css("div.property-title h1 a::text").get()
        rent            = make_rent(response.css("div.property-price .amount::text").get())    
        images          = response.css(".noo-property-gallery1 div ul li img::attr(src)").getall()
        currency        = "USD"
        address         = response.css("div.noo-property-content ::text").re(r"Address.*")
        available_date  = response.css("div.noo-property-content ::text").re(r"Status.*")
        property_type   = make_property(response.css("div.noo-property-content ::text").re(r"Property Type.*"))
        room_count      = make_room(response.css("div.noo-property-content ::text").re(r"Bedroom\(s\): [0-9]"))
        bathroom_counts = make_room(response.css("div.noo-property-content ::text").re(r"Bathroom\(s\): [0-9]"))
        fetures         = response.css("div.noo-property-content ::text").re(r"Features: .*")
        parking         = response.css("div.noo-property-content ::text").re(r"Parking:.*")
        landlord_email  = response.css("div.textwidget.custom-html-widget ul li::text")[1].get().strip()
        landlord_phone  = response.css("div.textwidget.custom-html-widget ul li::text")[2].get().strip()
        pets_allowed    = check_pets(response.css("div.noo-property-content ::text").re(r".*Pets.*"))
        description     = make_desc(response.css("div.noo-property-content p::text").getall())
        square_meters   = response.css("div.noo-property-content ::text").re(r"Grand Total.*")
        
        
        
        if not square_meters :
            square_meters = response.css("div.noo-property-content ::text").re(r"Floor Area.*")
        if not square_meters :
            square_meters = response.css("div.noo-property-content ::text").re(r"Subj. Space.*")
        
        
        if len(address) == 0:
            address       = response.css("div.noo-property-content ::text").getall()[3].replace("Address:","")
            if address == "Property Details:":
                address   = response.css("div.noo-property-content ::text").getall()[1]
        else:
            address       = address[0].replace("Address:","")

            



        washing_machine,swimming_pool = get_features(fetures)
        available_date, furnished     = get_status(available_date)
        square_meters                 = make_square_meters(square_meters)
        
        
        

        item = ListingLoader(response=response)
        
        item.add_value("external_source"        ,self.external_source)
        item.add_value("external_link"          ,response.url)
        item.add_value("title"                  ,title)
        item.add_value("available_date"         ,available_date)
        item.add_value("address"                ,address)
        item.add_value("rent"                   ,rent)
        item.add_value("images"                 ,images)
        item.add_value("property_type"          ,property_type)
        item.add_value("room_count"             ,room_count)
        item.add_value("bathroom_count"         ,bathroom_counts)
        item.add_value("swimming_pool"          ,swimming_pool)
        item.add_value("square_meters"          ,int(int(square_meters)*10.764))
        item.add_value("parking"                ,parking != "No Parking")
        item.add_value("furnished"              ,furnished)
        item.add_value("pets_allowed"           ,pets_allowed)
        item.add_value("washing_machine"        ,washing_machine)
        item.add_value("description"            ,description)
        item.add_value("currency"               ,currency)
        item.add_value("landlord_email"         ,landlord_email)
        item.add_value("landlord_phone"         ,landlord_phone)
        
        
        if property_type not in ["retail", "office"]:
        
            yield item.load_item()




def get_features(fetures):
    swimming_pool = ""
    washer        = ""
    if len(fetures) > 0:
        fetures = fetures[0]
        if "Swimming pool" in  fetures:
            swimming_pool = True
        if "ClthWsh" in fetures or "Clothes Washer" in fetures:
            washer =  True
    return washer, swimming_pool

def get_status(data):
    fur = ""
    if len(data) > 0:
        data = data[0].replace("Status: Available", "").replace("!","").strip()
        if "Unfurnished" in data:
            fur = False
            data = data.replace("Unfurnished", "").strip()
        if "Now" in data:
            return datetime.now().strftime("%Y-%m-%d"), fur
        else:
            data = dateutil.parser.parse(data).strftime("%Y-%m-%d")
        
            
    return data, fur




def make_square_meters(text):
    newtext = ""
    if len(text) > 0:
        text = text[0]
        newtext = re.findall("[0-9]+,*[0-9]*",text)
        if len(newtext) > 0:
            newtext = round(float(newtext[0].replace(",",""))*0.0929)
                
    return newtext




def make_rent(rent):
    if len(rent) > 0:
        rent = int(rent.replace(",","").strip())
    return rent

def make_property(prop):
    if len(prop)>0:
        prop = prop[0].lower()
        prop = re.findall(": [a-z]*",prop)[0].replace(":","").strip()
    
    if prop == "condo":
        return "apartment"
    return prop

def check_pets(pets):
    if len(pets)>0:
        if "No Pets" in pets[0]:
            return False
    return pets

def make_room(val):
    if len(val) > 0:
        val = val[0]
        val = int(re.findall("[0-9]+",val)[0])
    return val


def make_desc(desc):
    if len(desc) > 0:
        desc = " ".join(desc).replace("More properties available: http://sincerealty.com/en/contract_type/rent","").lower()

        
        desc = re.findall(r"at least.*required.", desc)
    return desc
