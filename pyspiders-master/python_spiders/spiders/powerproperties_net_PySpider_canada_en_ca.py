import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
from ..user_agents import random_user_agent

class powerproperties_net_PySpider_canadaSpider(scrapy.Spider):
    name = 'powerproperties_net'
    allowed_domains = ['powerproperties.net','powerproperties.rentals']
    start_urls = [
        'https://www.powerproperties.rentals/rentals'
        ]
    country = 'canada'
    locale = 'en_ca'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'
    custom_settings = {
        "PROXY_ON": "True"}

    def start_requests(self):
        yield Request(url='https://api.theliftsystem.com/v2/search?locale=en&client_id=581&auth_token=sswpREkUtyeYjeoahA2i&city_id=408&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=3000&min_sqft=0&max_sqft=10000&show_all_properties=true&show_custom_fields=true&show_promotions=true&region=&keyword=false&property_types=apartments%2C+houses&ownership_types=&exclude_ownership_types=&custom_field_key=&custom_field_values=&order=min_rate+ASC&limit=66&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false',
                    callback=self.parse,
                    body='',
                    method='GET')
    
    def parse(self, response):  
        parsed_response = json.loads(response.body)
        for item in parsed_response:
            yield Request(url=item['permalink'],callback=self.parse_property,
            meta={
                "external_id":str(item['id']),
                "title":item['name'],
                "property_type":item['property_type'],
                "landlord_name":item['client']['name'],
                "landlord_email":item['client']['email'],
                "landlord_phone":item['client']['phone'],
                "address":item['address']['address'],
                "city":item['address']['city'],
                "zipcode":item['address']['postal_code'],
                "latitude":item['geocode']['latitude'],
                "longitude":item['geocode']['longitude'],
                "description":item['details']['overview']
            })

    def parse_property(self, response):
        item_loader = ListingLoader(response=response)
        title = response.meta.get("title")
        description= response.meta.get("description")
        if "Virtual Tour" in description:
            description = description.split("Virtual Tour")[0]
        property_type = str(response.meta.get("property_type"))
        if 'House' in property_type or "Townhouse" in property_type:
            property_type = 'house'
        else:
            property_type = 'apartment'
        external_id = response.meta.get("external_id")
        landlord_name = response.meta.get("landlord_name")
        landlord_email = response.meta.get("landlord_email")
        landlord_phone = response.meta.get("landlord_phone")
        address = response.meta.get("address")
        city = response.meta.get("city")
        zipcode = response.meta.get("zipcdoe")
        latitude = response.meta.get("latitude")
        longitude = response.meta.get("longitude")

        images = response.css(".cover").extract()
        for i in range(len(images)):
                images[i]=images[i].split('data-src2x="')[1].split('" class=')[0]
        external_images_count = len(images)

        room_count = response.css(".suite-type::text").get().split("Bed")[0]
        if "One" in room_count:
            room_count = 1
        elif "Two" in room_count:
            room_count = 2
        else:
            room_count = int(room_count)
        bathroom_count = response.css(".suite-bath .value::text").get().strip()
        if ".5" in bathroom_count:
            bathroom_count = int(bathroom_count.replace(".5",""))
        rent = int(response.css(".suite-rate .value::text").get().replace("$","").strip())
        available_date = response.css(".suite-availability > a::text").get()
        
        amenities = response.css(".amenity-holder::text").extract()
        for i in range(len(amenities)):
            amenities[i] = amenities[i].strip()
        pets_allowed = None
        parking = None
        balcony = None
        terrace = None
        washing_machine = None
        dishwasher = None

        if "Pets Not Allowed" in amenities:
            pets_allowed = False
        if "Double Garage" in amenities or "parking" in description:
            parking = True
        if "Balcony" in amenities:
            balcony = True
        if "Patio/deck" in amenities:
            terrace = True
        if "Washer in suite" in amenities:
            washing_machine = True
        if "Dishwasher available" in amenities:
            dishwasher = True

        item_loader.add_value('external_link', response.url)
        item_loader.add_value('external_id',external_id)        
        item_loader.add_value('external_source', self.external_source)
        item_loader.add_value('title',title)
        item_loader.add_value('description',description)
        item_loader.add_value('city',city)
        item_loader.add_value('zipcode',zipcode)
        item_loader.add_value('address',address)
        item_loader.add_value('latitude',latitude)
        item_loader.add_value('longitude',longitude)
        item_loader.add_value('property_type',property_type)
        item_loader.add_value('room_count',room_count)
        item_loader.add_value('bathroom_count',bathroom_count)
        item_loader.add_value('available_date',available_date)
        item_loader.add_value('images',images)
        item_loader.add_value('external_images_count',external_images_count)
        item_loader.add_value('rent',rent)
        item_loader.add_value('currency','CAD')
        item_loader.add_value('pets_allowed',pets_allowed)
        item_loader.add_value('parking',parking)
        item_loader.add_value('balcony',balcony)
        item_loader.add_value('terrace',terrace)
        item_loader.add_value('washing_machine',washing_machine)
        item_loader.add_value('dishwasher',dishwasher)
        item_loader.add_value('landlord_name',landlord_name)
        item_loader.add_value('landlord_email',landlord_email)
        item_loader.add_value('landlord_phone',landlord_phone)
        yield item_loader.load_item()