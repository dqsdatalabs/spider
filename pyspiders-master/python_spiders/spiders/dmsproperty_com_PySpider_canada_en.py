import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
import math

class dmsproperty_com_PySpider_canadaSpider(scrapy.Spider):
    name = 'dmsproperty_com'
    allowed_domains = ['dmsproperty.com']
    start_urls = [
        'https://www.dmsproperty.com/apartments-for-rent/cities'
        ]
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'




    def start_requests(self):
        yield Request(url='https://api.theliftsystem.com/v2/search?locale=en&only_available_suites=&show_all_properties=&client_id=122&auth_token=sswpREkUtyeYjeoahA2i&city_ids=114,161,329,332,356,387,537,605,799,891,902,1174,1410,1425,1724,1818,1837,1975,1978,2042,2070,2072,2073,2081,2165,2377,2566,2870,2919,3106,3133,3303,3418&geocode=&min_bed=-1&max_bed=5&min_bath=-1&max_bath=10&min_rate=0&max_rate=10000&property_types=low-rise-apartment,mid-rise-apartment,high-rise-apartment,multi-unit-house,luxury-apartment,single-family-home,duplex,triplex,townhouse,rooms,semi&region=&keyword=&order=min_rate+ASC%2C+max_rate+ASC%2C+min_bed+ASC%2C+max_bed+ASC&limit=9999&offset=0&count=false',
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
                meta={'external_id':external_id,
                    'title':title,
                    'property_type': property_type,
                    'address': address,
                    'city': city,
                    'zipcode': zipcode,
                    'pets_allowed': pets_allowed,
                    'description': description,
                    'latitude':latitude,
                    'longitude':longitude,
                    'landlord_name':landlord_name,
                    'landlord_phone':landlord_phone,
                    'landlord_email':landlord_email})


    def parse_property(self, response):
        suite_types = response.css(".suite-info-container").extract()    
        for i in range(len(suite_types)):
            if "Available Now" in suite_types[i]:
                item_loader = ListingLoader(response=response)
                room_count = suite_types[i].split('Bedrooms')[1].split('</span>\n\t\t\t\t</li>')[0]
                room_count = int(room_count.split('"info">\n')[1].strip())
                bathroom_count = suite_types[i].split('Bathrooms')[1].split('</span>\n\t\t\t\t</li>')[0]
                bathroom_count = bathroom_count.split('"info">')[1].strip()
                if '.5' in bathroom_count:
                    bathroom_count = int(math.ceil(float(bathroom_count)))
                else:
                    bathroom_count = int(bathroom_count)
                square_meters = None
                if "sq. ft." in suite_types[i]:
                    square_meters = suite_types[i].split('Starting From')[1].split(' sq. ft.')[0]
                    square_meters = math.ceil(int(square_meters.split('"info">\n')[1].strip())/10.764)
                rent = suite_types[i].split('$')[1].split(' ')[0]
                if ',' in rent:
                    rent = int(rent.replace(',',''))
                else:
                    rent = int(rent)
                floor_plan_images = None
                try:
                    floor_plan_images = suite_types[i].split('data-pdf="')[1].split('">View')[0]
                except:
                    pass



                images = response.css(".cover").extract()
                for i in range(len(images)):
                    images[i] = images[i].split('data-src2x="')[1].split('" ')[0]
                external_images_count = len(images)


                amenities= response.css('.amenity-group:nth-child(1) .amenity-holder::text').extract()
                amenities2 = ["NOTHING HERE",'Nothing']
                try:
                    amenities_2= response.css('.amenity-group:nth-child(2) .amenity-holder::text').extract()
                    for i in range(len(amenities2)):
                        amenities2[i]=amenities2[i].replace('\n','').strip()
                except:
                    pass    
                for i in range(len(amenities)):
                    amenities[i]=amenities[i].replace('\n','').strip()
                dishwasher = None
                washing_machine = None
                balcony = None
                parking = None
                elevator = None
                parking = None

                if 'Outdoor parking' in amenities or 'Underground parking' in amenities or 'Outdoor parking' in amenities2 or 'Underground parking' in amenities2:
                    parking = True
                if 'Laundry Facilities' in amenities or 'Laundry Facilities' in amenities2:
                    washing_machine = True
                if 'Dishwasher' in amenities or 'Dishwasher' in amenities2:
                    dishwasher = True
                if 'Elevators' in amenities or 'Elevators' in amenities2:
                    elevator = True
                if 'Balconies' in amenities or 'Large Balconies' in amenities or 'Balconies' in amenities or 'Large Balconies' in amenities2:
                    balcony = True

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
                landlord_name = response.meta.get("landlord_name")
                landlord_phone = response.meta.get("landlord_phone")
                landlord_email = response.meta.get("landlord_email")

                terrace = None
                swimming_pool = None

                if 'Terrace' in description:
                    terrace = True
                if 'Balcony' in description or 'balconies' in description:
                    balcony = True
                if 'Parking is available' in description or 'Free Parking' in description:
                    parking = True
                if 'pool' in description:
                    swimming_pool = True
                if landlord_name is None:
                    landlord_name = 'dmsproperty'
                if landlord_phone is None:
                    landlord_phone = '416-661-3070'
                if landlord_email is None:
                    landlord_email = 'info@dmsproperty.com'
                if pets_allowed == "n/a":
                    pets_allowed = None
                property_type = property_type.lower()
                if 'house' in property_type or 'townhouse' in property_type:
                    property_type = 'house'
                else:
                    property_type = 'apartment'
                available_date = 'Available Now'
                if room_count == 0:
                    room_count = 1
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
                item_loader.add_value('square_meters',int(int(square_meters)*10.764))
                item_loader.add_value('room_count',room_count)
                item_loader.add_value('bathroom_count',bathroom_count)
                item_loader.add_value('available_date',available_date)
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
                yield item_loader.load_item()
