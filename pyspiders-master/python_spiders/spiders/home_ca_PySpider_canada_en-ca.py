import scrapy
from scrapy import Request
from ..loaders import ListingLoader

class home_ca_PySpider_canadaSpider(scrapy.Spider):
    name = 'home_ca'
    allowed_domains = ['home.ca']
    start_urls = [
        'https://home.ca/toronto-real-estate/houses-for-rent',
        'https://home.ca/toronto-real-estate/for-rent',
        'https://home.ca/toronto-real-estate/condos-for-rent',
        'https://home.ca/toronto-real-estate/townhouses-for-rent'
        ]
    country = 'canada'
    locale = 'en_ca'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    def parse(self, response):  
        urls = response.css("div.row.development-list > article > div > div.card-listing-image > a::attr(href)").extract()
        rent = response.css("body > div.container-fluid > div.row.development-list > article > div > div.caption > p.price-box > span.price::text").extract()
        for url in urls:
            yield Request(url=url,
            callback = self.parse_property,
            meta={'rent': rent})
        

    def parse_property(self, response):
        item_loader = ListingLoader(response=response)
        info = response.css("#devdetails").get()
        available_date = info.split('Availability Date:</td><td>')[1].split('</td>')[0]
        room_info = response.css(" div.well.devtitle.mls > div > div:nth-child(1) > div > small::text").get()
        room_count = int(room_info.split("Bed")[0])
        bathroom_count = int(room_info.split("•")[1].split("Bath")[0])
        washing_machine = None
        parking = None
        dishwasher = None
        pets_allowed = None
        if "Dishwasher" in info or "dishwasher" in info:
            dishwasher = True
        if "No Pets" in info:
            pets_allowed = False
        if "Washer" in info or "washer" in info or "laundry" in info or "Laundry" in info:
            washing_machine = True
        if "Parking Included:</td><td>Y" in info:
            parking = True
        external_id = None
        try:
            external_id = info.split("MLS®#:</td><td>")[1].split("</td></tr>")[0]
        except:
            pass
        property_type = response.css("body > div.container > div.row > div.col-md-4.col-lg-3 > div.well.status > div > ul > li:nth-child(1) > span::text").get()
        if "Apartment" in property_type or "apartment" in property_type:
            property_type = 'apartment'
        else:
            property_type = 'house'
        landlord_name = response.css("#devexpert > div > div > div > p::text").get()
        square_meters = None
        try:
            square_meters = info.split("Size (sq ft):</td><td>")[1].split("</td></tr> <tr><td>Property Type")[0].split("-")[1]
            square_meters = round(int(square_meters)/10.764,1)          
        except:
            pass
        rent= response.meta.get("rent")
        rent = int(rent[0].replace(",","").replace("$",""))
        title = response.css("#detail::text").get().strip()
        city = "Toronto"
        address = title+', '+response.css("#detail > small > a:nth-child(2)::text").get().strip() +", "+city
        description = response.css("#devdetails > div:nth-child(1) > div:nth-child(1) > p.nw::text").get()
        geo_info = response.css("body > script:nth-child(13)::text").get()
        latitude = geo_info.split('"latitude": ')[1].split('"longitude"')[0].split(",")[0]
        longitude = geo_info.split('"longitude": ')[1].split('}')[0].split("\n")[0]
        zipcode = geo_info.split('"postalCode": "')[1].split('"')[0]
        images = response.css("#photos > img::attr(src)").extract()
        external_images_count = len(images)

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
        item_loader.add_value('available_date', available_date)
        item_loader.add_value('images',images)
        item_loader.add_value('external_images_count',external_images_count)
        item_loader.add_value('rent',rent)
        item_loader.add_value('currency','CAD')
        item_loader.add_value('pets_allowed',pets_allowed)
        item_loader.add_value('parking',parking)
        item_loader.add_value('washing_machine',washing_machine)
        item_loader.add_value('dishwasher',dishwasher)
        item_loader.add_value('landlord_name',landlord_name)
        item_loader.add_value('landlord_email','(647) 800-2130')
        item_loader.add_value('landlord_phone','(647) 800-2130')
        yield item_loader.load_item()
        
