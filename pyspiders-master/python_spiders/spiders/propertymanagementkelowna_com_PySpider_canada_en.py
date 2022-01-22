import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
import requests
import math

class propertymanagementkelowna_com_PySpider_canadaSpider(scrapy.Spider):
    name = 'propertymanagementkelowna_com'
    allowed_domains = ['propertymanagementkelowna.com']
    start_urls = [
        'https://www.propertymanagementkelowna.com/available-properties/'
        ]
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    def parse(self, response):
        api_key = response.css("#pw-listing-widget::attr(data-customer-id)").get()
        api_key = "Apikey "+api_key
        endpoint= 'https://connect.propertyware.com/auth/apikey'
        data = {}
        headers = {"Authorization":api_key}
        bearer_key = str(requests.post(endpoint, data=data, headers=headers).json())
        bearer_key = bearer_key.split("'access-key': '")[1].split("'}")[0]
        endpoint= 'https://connect.propertyware.com/api/marketing/listings?website_id=594935808&widget_id=26771456&include_for_rent=true&include_for_sale=false'
        headers = {"Authorization":bearer_key}
        listing_data =requests.get(endpoint,data=data, headers=headers).json()
        for item in listing_data:
            item_loader = ListingLoader(response=response)
            external_id = str(item['id'])
            url = "https://www.propertymanagementkelowna.com/available-properties/property/"+external_id
            title = item['name']
            properties = item['property_type']
            address = item['address']
            city = item['city']
            zipcode = item['zip']
            pets_allowed = item['pets_allowed']
            description = item['description']
            available_date = item['available_date']
            latitude = str(item['lattitude'])
            longitude = str(item['longitude'])
            deposit = item['target_deposit']
            rent = int(item['target_rent'])
            square_meters = int(item['total_area'])
            room_count = int(item['no_bedrooms'])
            bathroom_count = str(item['no_bathrooms'])


            property_type = properties.lower()
            if "condo" in property_type or "other" in property_type or 'apartment' in property_type:
                property_type = "apartment"
            else:
                property_type = "house"
            amenities = []
            for i in range(len(item['amenities'])):
                extra_details = item['amenities'][i]['name']
                amenities.append(extra_details)
            images = []
            for i in range(len(item['images'])):
                image = item['images'][i]['original_image_url']
                images.append(image)
            external_images_count = len(images)
            if '.5' in bathroom_count:
                bathroom_count = int(math.ceil(float(bathroom_count)))
            else:
                bathroom_count = int(bathroom_count.replace('.0',''))
            square_meters = round(square_meters/10.764,1)
            if square_meters == 0 :
                square_meters = None
            if "," in deposit:
                deposit = deposit.replace("$","").replace(",","")
                if ".50" in deposit:
                    deposit = int(math.ceil(float(deposit)))
                else:
                    deposit = int(deposit.replace(".00",""))
            elif "$" in deposit:
                if ".50" in deposit:
                    deposit = int(math.ceil(float(deposit.replace("$",""))))
                else:
                    deposit = int(deposit.replace(".00","").replace("$",""))
            furnished = None
            parking = None
            balcony = None
            terrace = None
            swimming_pool = None
            washing_machine = None
            dishwasher = None

            if amenities is not None:
                if "Dishwasher" in amenities:
                    dishwasher = True
                if "Washer/Dryer In Unit" in amenities:
                    washing_machine = True
                if "Balcony" in amenities:
                    balcony = True
                if "Assigned Covered Parking" in amenities or "Assigned Outdoor Parking" in amenities:
                    parking = True
                if "Pool" in amenities:
                    swimming_pool = True
            
            if "Terrace" in description:
                terrace = True
            if "pool" in description:
                swimming_pool = True
            if "balcony" in description:
                balcony = True
            if "Non-furnished" in description or "Non-Furnished" in description or "Unfurnished" in description or "unfurnished" in description or "Un-Furnished" in description or "Not furnished" in description:
                furnished = False
            elif "furnished" in description or "Furnished" in description:
                furnished = True
            if "Laundry" in description or "laundry" in description:
                washing_machine = True
            if "Dishwasher" in description or "dishwasher" in description:
                dishwasher = True


            item_loader.add_value('external_link', url)      
            item_loader.add_value('external_source',self.external_source)
            item_loader.add_value('external_id',external_id)
            item_loader.add_value('title',title)
            item_loader.add_value('description',description)
            item_loader.add_value('city',city)
            item_loader.add_value('address',address)
            item_loader.add_value('zipcode',zipcode)
            item_loader.add_value('latitude',latitude)
            item_loader.add_value('longitude',longitude)
            item_loader.add_value('property_type',property_type)
            item_loader.add_value('square_meters',int(int(square_meters)*10.764))
            item_loader.add_value('room_count',room_count)
            item_loader.add_value('bathroom_count',bathroom_count)
            item_loader.add_value('available_date',available_date)
            item_loader.add_value('images',images)
            item_loader.add_value('external_images_count',external_images_count)
            item_loader.add_value('rent',rent)
            item_loader.add_value('currency','CAD')
            item_loader.add_value('deposit',deposit)
            item_loader.add_value('pets_allowed',pets_allowed)
            item_loader.add_value('furnished',furnished)
            item_loader.add_value('parking',parking)
            item_loader.add_value('balcony',balcony)
            item_loader.add_value('terrace',terrace)
            item_loader.add_value('swimming_pool',swimming_pool)
            item_loader.add_value('washing_machine',washing_machine)
            item_loader.add_value('dishwasher',dishwasher)
            item_loader.add_value('landlord_name','propertymanagementkelowna')
            item_loader.add_value('landlord_phone','250-868-3151')
            item_loader.add_value('landlord_email','rentals@vantagewestrealty.com')  
            yield item_loader.load_item()
            
        