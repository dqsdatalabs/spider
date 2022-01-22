import scrapy
import requests
import json
from python_spiders.helper import remove_white_spaces
import re
from python_spiders.loaders import ListingLoader
import dateutil.parser

class North44pmSpider(scrapy.Spider):
    name = 'north44pm'
    execution_type = 'testing'
    country = 'canada'
    locale = 'ca'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['north44pm.com','api.theliftsystem.com']
    start_urls = ['https://api.theliftsystem.com/v2/search?client_id=110&auth_token=sswpREkUtyeYjeoahA2i&city_id=2559&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=9999&show_all_properties=true&show_custom_fields=true&show_promotions=true&region=&keyword=false&property_types=&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=featured+DESC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false']
    position = 1

    def parse(self, response):
        list_cities = [
            'https://api.theliftsystem.com/v2/search?client_id=110&auth_token=sswpREkUtyeYjeoahA2i&city_id=2559&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=9999&show_all_properties=true&show_custom_fields=true&show_promotions=true&region=&keyword=false&property_types=&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=featured+DESC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false',
            'https://api.theliftsystem.com/v2/search?client_id=110&auth_token=sswpREkUtyeYjeoahA2i&city_id=3111&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=9999&show_all_properties=true&show_custom_fields=true&show_promotions=true&region=&keyword=false&property_types=&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=featured+DESC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false',
            'https://api.theliftsystem.com/v2/search?client_id=110&auth_token=sswpREkUtyeYjeoahA2i&city_id=3133&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=9999&show_all_properties=true&show_custom_fields=true&show_promotions=true&region=&keyword=false&property_types=&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=featured+DESC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false',
            'https://api.theliftsystem.com/v2/search?client_id=110&auth_token=sswpREkUtyeYjeoahA2i&city_id=22412&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=9999&show_all_properties=true&show_custom_fields=true&show_promotions=true&region=&keyword=false&property_types=&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=featured+DESC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false',
            'https://api.theliftsystem.com/v2/search?client_id=110&auth_token=sswpREkUtyeYjeoahA2i&city_id=1607&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=9999&show_all_properties=true&show_custom_fields=true&show_promotions=true&region=&keyword=false&property_types=&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=featured+DESC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false',
            'https://api.theliftsystem.com/v2/search?client_id=110&auth_token=sswpREkUtyeYjeoahA2i&city_id=1724&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=9999&show_all_properties=true&show_custom_fields=true&show_promotions=true&region=&keyword=false&property_types=&ownership_types=&exclude_ownership_types=&housing_types=&exclude_housing_types=&custom_field_key=&custom_field_values=&suite_availabilities=&order=featured+DESC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false',  
        ]

        for url in list_cities:
            data = json.loads(requests.get(url).text)
            for ad in data:
                dataUsage={
                "external_id"            :str(ad['id']),
                
                "title"                  :ad['name'],
                "property_type"          :make_prop(ad['property_type']),
                "landlord_name"          :ad['contact']['name'],
                "landlord_phone"         :ad['contact']['phone'],
                "landlord_email"         :ad['contact']['email'],
    
                "city"                   :ad['address']['city'],
                "zipcode"                :ad['address']['postal_code'],
                "address"                :ad['address']['address'],
                "latitude"               :ad['geocode']['latitude'],
                "longitude"              :ad['geocode']['longitude'],
                "pets_allowed"           :ad['pet_friendly'] == True,
                }
                yield scrapy.Request(url=ad['permalink'].replace("-for-rent",""), callback=self.parse_page, meta={'dataUsage':dataUsage})

    def parse_page(self, response):
        try:
            description = remove_white_spaces(response.css(".cms-content p::text").get())
        except:
            description = response.css(".cms-content p::text").get()
        images      = response.css(".gallery-images a::attr(href)").re(".*jpg$")
        balcony,dishwasher,washing_machine, elevator, parking, swimming_pool = fetch_amenities(response.css(".amenity::text").getall())
        counter = 1
        ad = response.meta['dataUsage']
        for apart in response.css(".suites .suite"):
            room_count      = apart.css(".suite-type::text").get()
            bathroom_count  = int(float(apart.css(".suite-bath .value::text").get()))
            square_meters   = apart.css(".suite-sqft .value::text").get()
            rent            = int(apart.css(".suite-rate .value::text").get().replace("$",""))
            available_date  = apart.css(".suite-status::text").get().strip()
            if "Available Now" in available_date:
                available_date = ""
            elif "Currently Unavailable" in available_date:
                continue
            else:
                available_date = dateutil.parser.parse(available_date.replace("Available:","").strip()).strftime("%Y-%m-%d")
            if square_meters:
                square_meters = int(square_meters.replace(",",""))
            if 'bachelor' in room_count.lower():
                room_count = 1
                property_type = 'studio'
            elif 'room' in room_count.lower():
                room_count = 1
                property_type  = 'room'
            else:
                room_count = re.findall("[0-9]\W*[Bb]edroom",room_count)
                if room_count:
                    room_count = int(float(room_count[0].lower().replace("bedroom","").strip()))

            landlord_name = ad['landlord_name']
            if not landlord_name:
                landlord_name = 'North44 Property Management'
            item = ListingLoader(response=response)
            item.add_value("external_link"          ,response.url+"#"+str(counter))
            item.add_value("external_source"        ,self.external_source)
            item.add_value("external_id"            ,ad['external_id'])
            item.add_value("position"               ,self.position) # Int
            item.add_value("title"                  ,ad['title'])
            item.add_value("address"                ,ad['address'])
            item.add_value("zipcode"                ,ad['zipcode'])
            item.add_value("city"                   ,ad['city'])
            item.add_value("latitude"               ,ad['latitude'])
            item.add_value("longitude"              ,ad['longitude'])
            item.add_value("property_type"          ,ad['property_type'])
            item.add_value("square_meters"          ,square_meters)
            item.add_value("room_count"             ,room_count)
            item.add_value("bathroom_count"         ,bathroom_count)
            item.add_value("description"            ,description)
            item.add_value("pets_allowed"           ,ad['pets_allowed'])
            item.add_value("currency"               ,"CAD")
            item.add_value("parking"                ,parking)
            item.add_value("images"                 ,images)
            item.add_value("balcony"                ,balcony)
            item.add_value("elevator"               ,elevator)
            item.add_value("rent"                   ,rent)
            item.add_value("dishwasher"             ,dishwasher)
            item.add_value("washing_machine"        ,washing_machine)
            item.add_value("swimming_pool"          ,swimming_pool)
            item.add_value("landlord_phone"         ,ad['landlord_phone'])
            item.add_value("landlord_name"          ,landlord_name)
            item.add_value("landlord_email"         ,ad['landlord_email'])
            self.position += 1
            counter+=1
            yield item.load_item()


def fetch_amenities(l):
    balcony,diswasher,washing_machine, elevator, parking, swimming_pool = '','','','','',''
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
            elif 'pool' in i:
                swimming_pool = True
            elif 'unfurnished' in i:
                furnished = False
            elif 'furnished' in i:
                furnished = True
    return balcony,diswasher,washing_machine, elevator, parking, swimming_pool


def make_prop(val):
    apartments  = ['apartment', 'condo', '2-storey','fourplex', 'condo apt', '3-storey', 'condo townhouse', 'co-op apt','loft','bungaloft','2 1/2 storey']
    houses      = ['detached', 'house', 'twnhouse', 'townhouse','bungalow','multi-level']
    studios     = ['studio', 'bachelor']
    if not val:
        return ''
    val =  val.lower()

    for house in houses:
        if house in val:
            return 'house'
    for aprt in apartments:
        if aprt in val:
            return 'apartment'
    for studio in studios:
        if studio in val:
            return 'studio'
    
