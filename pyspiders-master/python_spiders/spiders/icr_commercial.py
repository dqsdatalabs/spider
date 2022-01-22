import scrapy
from scrapy import Request
from ..items import ListingItem
from ..loaders import ListingLoader
import datetime
import dateparser
from ..helper import *
import requests

class IcrCommercialSpider(scrapy.Spider):
    name = 'icr_commercial'
    allowed_domains = ['icrcommercial.com']
    start_urls = ['https://rentals.icrcommercial.com/available-rentals/']
    execution_type = 'testing'
    country = 'canada'
    locale ='en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)

    def parse(self, response):
        for start_url in self.start_urls:
            yield Request(url=start_url,
                          callback=self.parse_area)

    def parse_area(self, response):
        area_urls = response.css('.property-name a::attr(href)').extract()
        for area_url in area_urls:
            yield Request(url=area_url,
                          callback=self.parse_pages)

    def parse_pages(self, response):
        items = ListingItem()

        external_link = str(response.request.url)
        external_id = response.css('script:contains("location_list")::text').get()
        coords = external_id.split(',"lat":')[1].split('}];')[0]
        coords = coords.split(',"lng":')
        latitude = coords[0]
        longitude = coords[1]
        latitude = latitude.replace('"', '')
        longitude = longitude.replace('"', '')
        external_id = external_id.split(' [{"id":')[1].split(',"infobox":" ')[0]


        description = response.css(".description li ::text , .description p ::text")[:-1].extract()
        if "\nVISIT OUR WEBSITE FOR MORE INFORMATION:" in description:
            description = description[:-3]
        description = ' '.join(description)
        if description == "":
            description = response.css(".description span ::text").extract()
            description = ' '.join(description)
        if 'Contact Information' in description:
            description = description.split('Contact Information')[0]
        if 'Please Contact' in description:
            description = description.split('Please Contact')[0]


        deposit = None
        try:
            deposit = int(description.split('$')[1].split(' Security Deposit')[0])
        except:
            pass
        try:
            deposit = int(description.split(' Deposit $')[1].split('.00')[0])
        except:
            pass
        try:
            deposit = int(description.split('Security Deposit $')[1].split('.00')[0])
        except:
            pass

        title = response.css(".listing-name-description::text")[0].extract()
        square_feet = response.css(".size::text").extract()
        square_feet = square_feet[0]
        if any(char.isdigit() for char in square_feet) :
            square_feet = int(''.join(x for x in square_feet if x.isdigit()))
            square_feet = int(square_feet / 10.7639)
        else:
            square_feet = " "

        property_type = 'apartment'
        room_count = response.css(".beds::text").extract()
        room_count = room_count[0][1]
        room_count = int(room_count)
        if room_count == 0:
            property_type = 'studio'
            room_count = 1
        bathroom_count = response.css(".baths::text").extract()
        bathroom_count = bathroom_count[0][1]
        bathroom_count = int(bathroom_count)
        rent = response.css(".price::text")[0].extract()
        rent = rent.replace('/MON','')
        rent = rent.replace('$','')
        if not rent:
            return
        if '.' in rent:
            rent = rent[:-3]
        rent = int(rent)
        try:
            available_date = response.css(".available-date::text")[0].extract()
            available_date = available_date.strip()
            available_date = dateparser.parse(available_date)
            available_date = available_date.strftime("%Y-%m-%d")
        except:
            available_date = " "
        currency = "CAD"

        pets_allowed = response.css(".pets::text")[0].extract()
        if 'Pet Friendly' in pets_allowed :
            pets_allowed = True
        else:
            pets_allowed = None

        amenities = response.css(".amenity::text").extract()
        amenities = ''.join(amenities)
        balcony = None
        parking = None
        dishwasher = None
        washing_machine = None
        elevator = None
        swimming_pool = None
        try:
            if 'Deck/Balcony' in amenities:
                balcony = True
        except:
            pass
        try:
            if 'Garage' in amenities or 'Underground Parking' in amenities or 'Surface Parking' in amenities or 'Visitor Parking' in amenities:
                parking = True
        except:
            pass
        try:
            if 'Dishwasher' in amenities:
                dishwasher = True
        except:
            pass
        try:
            if 'Washer and Dryer' in amenities or 'Shared laundry' in amenities or 'laundry' in amenities.lower():
                washing_machine = True
        except:
            pass
        try:
            if 'Elevator' in amenities:
                elevator = True
        except:
            pass
        try:
            if 'Swimming Pool' in amenities:
                swimming_pool = True
        except:
            pass


        responseGeocode = requests.get(
            f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()

        address = responseGeocodeData['address']['Match_addr']
        city = responseGeocodeData['address']['City']
        zipcode = responseGeocodeData['address']['Postal']

        landlord_name = response.css(".name::text").extract()
        landlord_name = landlord_name[0]
        landlord_phone = response.css(".contact-number::attr(href)")[0].extract()
        landlord_phone = landlord_phone[4:]
        if any(char.isdigit() for char in landlord_phone) :
            landlord_phone = ''.join(x for x in landlord_phone if x.isdigit())
        else:
            try:
                landlord_phone = response.css("ul:nth-child(12) li")[0].extract()
                if any(char.isdigit() for char in landlord_phone):
                    landlord_phone = ''.join(x for x in landlord_phone if x.isdigit())
            except:
                landlord_phone = '3067216118'
        landlord_phone = landlord_phone[:10]
        landlord_email = response.css(".email::attr(href)").extract()
        landlord_email = landlord_email[0]
        images = response.css('.avia-slide-wrap img::attr(src)').extract()

        items['external_source'] = self.external_source
        items['external_link'] = external_link
        items['external_id'] = external_id
        items['address'] = address
        items['zipcode'] = zipcode
        items['longitude'] = longitude
        items['latitude'] = latitude
        items['title'] = title
        items['description'] = description
        items['property_type'] = property_type
        if square_feet != " ":
            items['square_meters'] = int(int(square_feet*10.764))
        items['room_count'] = room_count
        items['bathroom_count'] = bathroom_count
        items['rent'] = rent
        if available_date != " ":
            items['available_date'] = available_date
        items['elevator'] = elevator
        items['washing_machine'] = washing_machine
        items['dishwasher'] = dishwasher
        items['pets_allowed'] = pets_allowed
        items['parking'] = parking
        items['balcony'] = balcony
        items['swimming_pool'] = swimming_pool
        items['deposit'] = deposit
        items['city'] = city
        items['currency'] = currency

        items['landlord_name'] = landlord_name
        items['landlord_phone'] = landlord_phone
        items['landlord_email'] = landlord_email

        items['images'] = images


        yield items
