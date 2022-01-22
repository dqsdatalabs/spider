import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
import math

class greenwin_ca_PySpider_canadaSpider(scrapy.Spider):
    name = 'greenwin_ca'
    allowed_domains = ['greenwin.ca']
    start_urls = [
        'https://www.greenwin.ca/apartments-for-rent'
        ]
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'


    def start_requests(self):
        yield Request(url='https://api.theliftsystem.com/v2/search?client_id=2&auth_token=sswpREkUtyeYjeoahA2i&city_id=3396&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=6000&min_sqft=0&max_sqft=70000&region=&keyword=false&property_types=apartments%2C+houses&custom_field_key=&custom_field_values=&exclude_custom_field_key=is_planning&exclude_custom_field_values=Yes&suite_availabilities=available%2C+waitlist%2C+available_date&order=featured+DESC&limit=500&neighbourhood=&amenities=&promotions=&city_ids=1174%2C799%2C2015%2C415%2C1154%2C2566%2C3133%2C1425%2C329%2C1837%2C831%2C3284%2C902%2C161%2C2165%2C114%2C2042%2C3396&show_custom_fields=1&show_promotions=false&show_amenities=false&show_utilities=true&pet_friendly=&offset=0&count=false',
                    callback=self.parse,
                    body='',
                    method='GET')

    def parse(self, response):
        parsed_response = json.loads(response.body)
        for item in parsed_response:
            url = item['permalink']
            external_id = str(item['id'])
            title = item['name']
            property_type = item['property_type']
            address = item['address']['address']
            city = item['address']['city']
            zipcode = item['address']['postal_code']
            pets_allowed = item['pet_friendly']
            description = item['details']['overview']
            latitude = item['geocode']['latitude']
            longitude = item['geocode']['longitude']
            landlord_name = None
            landlord_phone = None
            landlord_email = None
            suite_features = item['custom_fields']['suite_features']
            property_features = item['custom_fields']['property_features'] 
            parking_a = 0
            parking_b = 0
            parking_c = 0
            try:
                parking_a = int(item['parking']['indoor'])
            except:
                pass
            try:
                parking_b = int(item['parking']['outdoor'])
            except:
                pass
            try:
                parking_c = int(item['parking']['additional'])
            except:
                pass
            parking = parking_a + parking_b + parking_c  
            if parking > 0:
                parking = True
            else:
                parking = None   
            try:
                landlord_name = item['contact']['name']
                landlord_phone = item['contact']['phone']
                landlord_email = item['contact']['email']
            except:
                pass
            if landlord_name == "":
                landlord_name = None
            availability = item['availability_status_label']
            if 'Available Now' in availability:
                yield Request(url=url, callback=self.parse_property,
                meta={'suite_features':suite_features,
                    'property_features':property_features,
                    'external_id':external_id,
                    'title':title,
                    'property_type': property_type,
                    'address': address,
                    'city': city,
                    'zipcode': zipcode,
                    'pets_allowed': pets_allowed,
                    'description': description,
                    'parking':parking,
                    'latitude':latitude,
                    'longitude':longitude,
                    'landlord_name':landlord_name,
                    'landlord_phone':landlord_phone,
                    'landlord_email':landlord_email})

    def parse_property(self, response):
        suite_rooms = response.css("body > section.top-content > div > div.main-content > div.mainbar > div.suites-container > section > div > div > div.content.no-photos").extract()
        hashtag = 1
        for i in range(len(suite_rooms)):
            if "Waiting List" not in suite_rooms[i]:
                item_loader = ListingLoader(response=response)
                room_count = None
                try:   
                    room_count = suite_rooms[i].split('"suite-type">')[1].split('</h3>')[0]
                    if 'Large' in room_count:
                        room_count = room_count.replace("Large","")
                    if 'Bedroom' in room_count:
                        if 'Jr' in room_count or 'Executive' in room_count:
                            room_count = 1
                        else:
                            room_count = int(room_count.split('Bedroom')[0])
                    else:
                        room_count = 1
                except:
                    pass
                if room_count is None:
                    room_count = 1
                bathroom_count = suite_rooms[i].split('suite-bath">')[1].split('</div>')[0]
                bathroom_count = bathroom_count.split('Bath')[0]
                if '.5' in bathroom_count:
                    bathroom_count = int(math.ceil(float(bathroom_count)))
                else:
                    bathroom_count = int(bathroom_count)
                square_meters = None
                try:
                    square_meters = suite_rooms[i].split('SQFT</span> ')[1].split('</div>')[0].strip()
                    square_meters = math.ceil(int(square_meters)/10.764)
                except:
                    pass
                rent = suite_rooms[i].split('$')[1].split('/')[0]
                if '.' in rent:
                    rent = int(math.ceil(float(rent)))
                else:
                    rent = int(rent)
                floor_plan_images = None
                try:
                    floor_plan_images = suite_rooms[i].split('" href="')[1].split('" ')[0]
                    if 'https://s3.amazonaws.com/lws_lift/greenwin/' not in floor_plan_images:
                        floor_plan_images = 'https://s3.amazonaws.com/lws_lift/greenwin7'+ floor_plan_images
                except:
                    pass
                
                suite_features = response.meta.get("suite_features")
                property_features = response.meta.get("property_features")
                external_id = response.meta.get("external_id")
                title = response.meta.get("title")
                property_type = response.meta.get("property_type")
                address = response.meta.get("address")
                city = response.meta.get("city")
                zipcode = response.meta.get("zipcode")
                pets_allowed = response.meta.get("pets_allowed")
                description = response.meta.get("description")
                latitude = response.meta.get("latitude")
                longitude = response.meta.get("longitude")
                parking = response.meta.get("parking")
                landlord_name = response.meta.get("landlord_name")
                landlord_phone = response.meta.get("landlord_phone")
                landlord_email = response.meta.get("landlord_email")
                if landlord_name is None:
                    landlord_name = 'Greenwin'
                if landlord_phone is None:
                    landlord_phone = '(416) 487-3883'
                if landlord_email is None:
                    landlord_email = 'feedback@greenwin.ca'
                if pets_allowed == "n/a":
                    pets_allowed = None
                property_type = property_type.lower()
                if 'house' in property_type or 'townhouse' in property_type:
                    property_type = 'house'
                else:
                    property_type = 'apartment'


                property_features = property_features.replace('&lt;li&gt;','').replace('&lt;/li&gt;','')
                suite_features = suite_features.replace('&lt;li&gt;','').replace('&lt;/li&gt;','')
                images = response.css("#flexslider-default-id-0 img").extract()
                for i in range(len(images)):
                    images[i]=images[i].split('src="')[1].split('" ')[0]
                external_images_count = len(images)
                elevator = None
                washing_machine = None
                dishwasher = None
                swimming_pool = None
                balcony = None
                terrace = None
                if 'Balconies' in property_features or 'Balcony' in description:
                    balcony = True
                if 'Terrace' in description:
                    terrace = True
                if 'Elevators' in property_features:
                    elevator = True
                if 'On-site laundry facility' in property_features or 'Laundry' in property_features:
                    washing_machine = True
                if 'indoor pool' in description:
                    swimming_pool = True


                item_loader.add_value('external_link', response.url+f"#{hashtag}")  
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
                item_loader.add_value('square_meters',int(int(square_meters)*10.764))
                item_loader.add_value('room_count',room_count)
                item_loader.add_value('bathroom_count',bathroom_count)
                item_loader.add_value('images',images)
                item_loader.add_value('external_images_count',external_images_count)
                item_loader.add_value('floor_plan_images',floor_plan_images)
                item_loader.add_value('rent',rent)
                item_loader.add_value('currency','CAD')
                item_loader.add_value('pets_allowed',pets_allowed)
                item_loader.add_value('parking',parking)
                item_loader.add_value('elevator',elevator)
                item_loader.add_value('balcony',balcony)
                item_loader.add_value('terrace',terrace)
                item_loader.add_value('swimming_pool',swimming_pool)
                item_loader.add_value('washing_machine',washing_machine)
                item_loader.add_value('dishwasher',dishwasher)
                item_loader.add_value('landlord_name',landlord_name)
                item_loader.add_value('landlord_phone',landlord_phone)
                item_loader.add_value('landlord_email',landlord_email)
                hashtag = hashtag + 1
                yield item_loader.load_item()
