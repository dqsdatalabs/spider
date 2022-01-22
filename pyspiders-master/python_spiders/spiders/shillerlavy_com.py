import requests
import scrapy
from scrapy import Request

from ..helper import sq_feet_to_meters, remove_unicode_char, format_date, extract_number_only
from ..loaders import ListingLoader

class WebsiteDomainSpider(scrapy.Spider):
    name = 'shillerlavy_com'
    allowed_domains = ['shillerlavy.com']
    start_urls = ['https://www.shillerlavy.com/leasing/residential/']
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    def parse(self, response):
        for start_url in self.start_urls:
            yield Request(url=start_url,
                          callback=self.parse_area)

    def parse_area(self, response):
        rentals = response.css('.property-fulladdress a::attr(href)').extract()
        for rental in rentals:
            yield Request(url=rental,
                          callback=self.parse_area_pages)

    def parse_area_pages(self, response):
        item_loader = ListingLoader(response=response)
        external_link = response.url
        external_id = None
        external_source = self.external_source
        title = response.css('.title-post::text').extract_first()
        description = remove_unicode_char((((response.css('.main_txt, .main_txt a, .main_txt li').extract())[0].replace('\n','')).replace('\t','')).replace('\r',''))
        address = remove_unicode_char(((response.css('.header-address::text').extract_first().replace('\n','')).replace('\t','')).replace('\r',''))
        responseGeocode = requests.get(
            f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find?f=json&text={address}&maxLocations=1")
        responseGeocodeData = responseGeocode.json()

        longitude = responseGeocodeData['locations'][0]['feature']['geometry']['x']
        latitude = responseGeocodeData['locations'][0]['feature']['geometry']['y']

        responseGeocode = requests.get(
            f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        zipcode = responseGeocodeData['address']['Postal']
        city = responseGeocodeData['address']['City']

        longitude = str(longitude)
        latitude = str(latitude)
        if 'studio' in title.lower():
            property_type = 'studio'
        elif 'house' in title.lower():
            property_type = 'house'
        elif 'apartment' in title.lower():
            property_type = 'apartment'
        else:
            property_type = 'apartment'
        square_meters = sq_feet_to_meters(response.css('.surface .meta-data::text').extract_first())
        room_count = None
        bathroom_count = None
        counter = 0
        desc_arr = description.split(" ")
        for i in desc_arr:
            if "bedroom" in i:
                room_count = extract_number_only(desc_arr[counter-1])
                if str(room_count).isnumeric():
                    break
                else:
                    room_count = 0
            else:
                counter +=1
        if (room_count == 0 and counter > 0) or room_count is None:
            room_count =1

        counter = 0
        for i in desc_arr:
            if "bathroom" in i:
                bathroom_count = extract_number_only(desc_arr[counter-1])
                if str(bathroom_count).isnumeric():
                    break
                else:
                    bathroom_count = 0
            else:
                counter +=1
        if (bathroom_count == 0 and counter > 0) or (bathroom_count is None):
            bathroom_count =1
        months_num = {1: "January", 2: "February", 3: "March", 4: "April", 5: "May", 6: "June", \
                         7: "July", 8: "August", 9: "September", 10: "October", 11: "November", 12: "December"}
        months_name = {v: k for k, v in months_num.items()}
        available_date = response.css('.availability .meta-data::text').extract_first()
        if available_date !='Immediately':
            available_date = available_date.replace(", ",'/')
            available_date = available_date.replace(" ",'/')
            available_date = available_date.split('/')
            formatted_date = available_date[1] + '/' + str(months_name[available_date[0]]) +'/'+ available_date[-1]
            available_date = format_date(formatted_date)
        images = response.css('.one-photo-wrapper a::attr(href)').extract()
        external_images_count = len(images)
        rent = int((response.css('.price .meta-data::text').extract_first())[1:])
        currency = "CAD"

        if 'furnish' in description.lower():
            furnished = True
        else:
            furnished = False

        floor = None
        if 'parking' in description.lower():
            parking = True
        else:
            parking = False

        if 'balcony' in description.lower():
            balcony = True
        else:
            balcony = False

        if 'terrace' in description.lower():
            terrace = True
        else:
            terrace = False

        swimming_pool = None

        if ' washer' in description.lower():
            washing_machine = True
        else:
            washing_machine = False

        if 'dishwasher' in description.lower():
            dishwasher = True
        else:
            dishwasher = False


        # --------------------------------#
        # item loaders
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('external_id', external_id)
        item_loader.add_value('external_source', external_source)
        item_loader.add_value('title', title)
        item_loader.add_value('description', description)
        item_loader.add_value('city', city)
        item_loader.add_value('zipcode', zipcode)
        item_loader.add_value('address', address)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value('property_type', property_type)
        item_loader.add_value('square_meters', int(int(square_meters)*10.764))
        item_loader.add_value('room_count', room_count)
        item_loader.add_value('bathroom_count', bathroom_count)
        item_loader.add_value('available_date', available_date)
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", external_images_count)
        item_loader.add_value("rent", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("floor", floor)
        item_loader.add_value("parking", parking)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("terrace", terrace)
        item_loader.add_value("swimming_pool", swimming_pool)
        item_loader.add_value("washing_machine", washing_machine)
        item_loader.add_value("dishwasher", dishwasher)
        item_loader.add_value("landlord_name", 'shiler lavy')
        item_loader.add_value("landlord_email", 'info@shilerlavy.com')
        item_loader.add_value("landlord_phone", '514-259-2400')

        yield item_loader.load_item()
