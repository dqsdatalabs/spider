import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json


class chrisclarketeam_com_PySpider_canadaSpider(scrapy.Spider):
    name = 'chrisclarketeam_com'
    allowed_domains = ['chrisclarketeam.com','gallery.remarketer.ca']
    start_urls = [
        'https://chrisclarketeam.com/available-listings'
        ]
    country = 'canada'
    locale = 'en_ca'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'


    def start_requests(self):
        yield Request(url='https://chrisclarketeam.com/api/availablebrokeragelistings?b=RE/MAX%20HALLMARK%20REALTY%20LTD.,%20BROKERAGE&limit=&sold=0&salelease=&class=Residential,Condo&proptype=Residential,Condo&accesslevel=REMEX,APOSOL,VOW&muni=&minprice=&maxprice=',
                    callback=self.parse,
                    body='',
                    method='GET')

    def parse(self, response):
        parsed_response = json.loads(response.body)
        for item in parsed_response:
            isLease = item['SaleLease']
            if "Lease" in isLease:
                mls = item['MLS']
                url = item['address']
                url1 = url.split(" ")[0]
                url1 = url1+"-"
                url2 = url.split(" ")[1]
                url2 = url2.replace(" ","%20")
                url = url1+url2
                url = "https://chrisclarketeam.com/Details/"+mls+"/"+url
                yield Request(url=url, callback=self.parse_property,
                meta={'title':item['address'],
                'external_id':item['MLS'],
                'city':item['Municipality'],
                'address':item['address'],
                'zipcode':item['PostalCode'],
                'rent':item['listPriceDecimal'],
                'room_count':int(item['Bedrooms']),
                'bathroom_count':int(item['Washrooms']),
                'parking':int(item['ParkingSpaces']),
                'property_type':item['PropertyType'],
                'square_meters':item['ApproxSquareFootage']
                })

    def parse_property(self, response):
        item_loader = ListingLoader(response=response)
        title = response.meta.get("title")
        external_id = response.meta.get("external_id")
        property_type = response.meta.get("property_type")
        address = response.meta.get("address")
        city = response.meta.get("city")
        zipcode = response.meta.get("zipcode")
        rent = response.meta.get("rent")
        square_meters = response.meta.get("square_meters")
        room_count = response.meta.get("room_count")
        bathroom_count = response.meta.get("bathroom_count")
        parking = response.meta.get("parking")

        description = response.css("#main > article > div > div.property-content").get().replace('<div class="property-content">','').replace('<p>','').replace('</p>','').replace('<','').replace('>','').replace('/','').replace('div','').replace('\n','').replace('  ','').strip()
        if 'Residential' in property_type:
            property_type = 'house'
        else:
            property_type = 'apartment'
        try:
            square_meters = round(int(square_meters.split('-')[1])/10.764,1)
        except:
            pass
        if parking > 0:
            parking = True
        if parking == 0:
            parking = False
        script = response.css("body > script").extract()
        for i in range(len(script)):
            if 'else{\r\n            latitude' in script[i]:
                latlng = script[i].split('else{\r\n            latitude = ')[1].split('; \r\n            loadMap')[0]
                latitude = latlng.split(';')[0]
                longitude = latlng.split(';\r\n             longitude = ')[1]
        
        images_info = response.css("a::attr(href)").extract()
        images = []
        for i in range(len(images_info)):
            if "https://gallery.remarketer.ca/VOW/tr:" in images_info[i]:
                images.append(images_info[i])
        external_images_count = len(images)

        furnished = None
        pets_allowed = None
        balcony = None
        terrace = None
        swimming_pool = None
        washing_machine = None
        dishwasher = None
        if 'Pet Friendly' in description:
            pets_allowed = True
        if 'No Pet' in description or 'No Pets' in description or 'Restriction For Pets' in description:
            pets_allowed = False
        if 'Furnished' in description or 'Patio Furniture' in description:
            furnished = True
        if 'Balcony' in description:
            balcony = True
        if 'Terrace' in description:
            terrace = True
        if 'Pool' in description:
            swimming_pool = True
        if 'Dishwasher' in description:
            dishwasher = True
        if 'Laundry' in description or 'Washer' in description:
            washing_machine = True


        item_loader.add_value('external_link', response.url)  
        item_loader.add_value('external_id',external_id)      
        item_loader.add_value('external_source', self.external_source)
        item_loader.add_value('title',title)
        item_loader.add_value('description',description)
        item_loader.add_value('city',city)
        item_loader.add_value('address',address)
        item_loader.add_value('zipcode',zipcode)
        item_loader.add_value('latitude',latitude)
        item_loader.add_value('longitude',longitude)
        item_loader.add_value('property_type',property_type)
        item_loader.add_value('rent',rent)
        item_loader.add_value('currency','CAD')
        item_loader.add_value('square_meters',int(int(square_meters)*10.764))
        item_loader.add_value('room_count',room_count)
        item_loader.add_value('bathroom_count',bathroom_count)
        item_loader.add_value('images',images)
        item_loader.add_value('external_images_count',external_images_count)
        item_loader.add_value('parking',parking)
        item_loader.add_value('pets_allowed',pets_allowed)
        item_loader.add_value('furnished',furnished)
        item_loader.add_value('balcony',balcony)
        item_loader.add_value('terrace',terrace)
        item_loader.add_value('swimming_pool',swimming_pool)
        item_loader.add_value('washing_machine',washing_machine)
        item_loader.add_value('dishwasher',dishwasher)
        item_loader.add_value('landlord_name','chrisclarketeam')
        item_loader.add_value('landlord_phone','416-414-5280')
        item_loader.add_value('landlord_email','chris@chrisclarketeam.com')
        # item_loader.add_value(,)
        yield item_loader.load_item()
