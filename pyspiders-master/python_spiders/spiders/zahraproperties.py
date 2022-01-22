import scrapy
import requests
import json
from ..loaders import ListingLoader
import dateutil.parser
class ZahrapropertiesSpider(scrapy.Spider):
    name = 'zahraproperties'
    execution_type = 'testing'
    country = 'canada'
    locale = 'ca'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['zahraproperties.com']
    start_urls = ['https://api.theliftsystem.com/v2/search?locale=en&client_id=598&auth_token=sswpREkUtyeYjeoahA2i&city_id=1837&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=6500&min_sqft=0&max_sqft=10000&show_custom_fields=true&show_promotions=true&show_amenities=true&region=&is_furnished=false&keyword=false&property_types=apartments%2C+houses%2C+commercial&ownership_types=&exclude_ownership_types=&custom_field_key=&custom_field_values=&order=min_rate+ASC&limit=66&neighbourhood=&amenities=&promotions=&city_ids=3133%2C1837&pet_friendly=&offset=0&count=false']
    position = 1

    def parse(self, response):
        start_url = 'https://api.theliftsystem.com/v2/search?locale=en&client_id=598&auth_token=sswpREkUtyeYjeoahA2i&city_id=1837&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=6500&min_sqft=0&max_sqft=10000&show_custom_fields=true&show_promotions=true&show_amenities=true&region=&is_furnished=false&keyword=false&property_types=apartments%2C+houses%2C+commercial&ownership_types=&exclude_ownership_types=&custom_field_key=&custom_field_values=&order=min_rate+ASC&limit=66&neighbourhood=&amenities=&promotions=&city_ids=3133%2C1837&pet_friendly=&offset=0&count=false'
        res = requests.get(start_url)
        
        for ad in json.loads(res.text):
            dataUsage={
                "external_source"        :self.external_source,
                # "external_id"            :str(ad['id']),
                
                "title"                  :ad['name'],
                "property_type"          :ad['property_type'],
                "landlord_name"          :ad['contact']['name'],
                "landlord_phone"         :ad['contact']['phone'],
                "landlord_email"         :'arnab@zahraproperties.com',
    
                "city"                   :ad['address']['city'],
                "zipcode"                :ad['address']['postal_code'],
                "address"                :ad['address']['address'],
                "latitude"               :ad['geocode']['latitude'],
                "longitude"              :ad['geocode']['longitude'],
            }
            yield scrapy.Request(url=ad['permalink'], callback=self.parse_page, meta={'dataUsage':dataUsage})

    def parse_page(self, response):
        images      = response.css("section.requires-js.slickslider_container div.has2x::attr(data-src2x)").getall()
        description = response.css(".container .main p::text").get()

        balcony,dishwasher,washing_machine, elevator, parking, pets_allowed, swimming_pool = fetch_amenities([i.strip() for i in response.css(".amenity-holder::text").getall()])
        parking = response.xpath('//h2[contains(text(), "Parking")]/text()').get()
        if parking:
            parking = True
        counter = 1
        # external_link = response.meta['external_link']
        ad = response.meta['dataUsage']
        for property in response.css(".suite-row"):
            available_date  = property.css(".availability-block .info a::text").get()
            if available_date != None:
                if "waitlist" not in available_date.lower():
                    if "available now" not in available_date.lower():
                        available_date = dateutil.parser.parse(available_date).strftime("%Y-%m-%d")
                    else:
                        available_date = ""
                    room_count      = int(float(property.css(".info-block .info::text").getall()[0].strip()))
                    bathroom_count  = int(float(property.css(".info-block .info::text").getall()[1].strip()))
                    square_meters   = int(property.css(".info-block .info::text").getall()[2].strip())
                    external_id     = property.css(".info-block .info::text").getall()[3].strip()
                    rent            = int(property.css(".info-block .info::text").getall()[4].strip())
                    furnished       = property.css(".furnished-info-block .info::text").get() != None

            


                    item = ListingLoader(response=response)
                    item.add_value("external_source"        ,self.external_source)
                    item.add_value("title"                  ,ad['title'])
                    item.add_value("property_type"          ,make_property_type(ad['property_type']))
                    item.add_value("landlord_name"          ,ad['landlord_name'])
                    item.add_value("landlord_phone"         ,ad['landlord_phone'])
                    item.add_value("landlord_email"         ,'arnab@zahraproperties.com')    
                    item.add_value("city"                   ,ad['city'])
                    item.add_value("zipcode"                ,ad['zipcode'])
                    item.add_value("address"                ,ad['address'])
                    item.add_value("latitude"               ,ad['latitude'])
                    item.add_value("longitude"              ,ad['longitude'])
                    item.add_value("position"               ,self.position)
                    item.add_value("external_link"          ,response.url+"#"+str(counter))
                    item.add_value("available_date"         ,available_date)
                    item.add_value("external_id"            ,external_id)
                    item.add_value("washing_machine"        ,washing_machine)
                    item.add_value("swimming_pool"          ,swimming_pool)
                    item.add_value("pets_allowed"           ,pets_allowed) 
                    item.add_value("square_meters"          ,square_meters)
                    item.add_value("room_count"             ,room_count)
                    item.add_value("bathroom_count"         ,bathroom_count)
                    item.add_value("currency"               ,"CAD")
                    item.add_value("parking"                ,parking)
                    item.add_value("images"                 ,images)
                    item.add_value("balcony"                ,balcony)
                    item.add_value("elevator"               ,elevator)
                    item.add_value("rent"                   ,rent)
                    item.add_value("description"            ,description)
                    item.add_value("dishwasher"             ,dishwasher)
                    item.add_value("images"                 ,images)
                    item.add_value("furnished"              ,furnished)
                    item.add_value("description"            ,description)
                    counter += 1
                    self.position += 1
                    yield item.load_item()

        
    

def fetch_prop_data(labels, infos):
    pass

def fetch_amenities(l):
    balcony,diswasher,washing_machine, elevator, parking, pets_allowed, swimming_pool = '','','','','','',''
    for i in l:
        if i:
            i = i.lower()
            if 'balcony' in i:
                balcony = True
    
            elif 'dishwasher' in i:
                diswasher = True
            
            elif 'washer' in i or 'laundry' in i:
                washing_machine = True
            
            elif 'parking' in i:
                parking = True
    
            elif 'elevator' in i:
                elevator = True
            elif "pets" in i:
                pets_allowed = True
            elif 'pool' in i:
                swimming_pool = True
    return balcony,diswasher,washing_machine, elevator, parking, pets_allowed, swimming_pool


def make_property_type(word):
    apartments = ['apartment']
    houses = ['house']
    studios = ['studio', 'bachelor']

    for apart in apartments:
        if apart in word.lower():
            return'apartment'
                  
    for house in houses:
        if  house in  word.lower() :
            return 'house'
    for studio in studios:
        if  studio in  word.lower() :
            return 'studio'
    return word
