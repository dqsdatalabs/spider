import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import math

class sleepwellmanagement_com_PySpider_canadaSpider(scrapy.Spider):
    name = 'sleepwellmanagement_com'
    allowed_domains = ['sleepwellmanagement.com']
    start_urls = [
        'https://sleepwellmanagement.com/property-type/multi-units/',
        'https://sleepwellmanagement.com/property-type/room/',
        'https://sleepwellmanagement.com/property-type/townhomes/'
        ]
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    def parse(self, response):  #page_follower
        urls = response.css(".mkdf-block-drag-link::attr(href)").extract()
        for url in urls:
            yield Request(url=url,
            callback = self.parse_property)

    def parse_property(self, response):
        item_loader = ListingLoader(response=response)
        square_meters = None
        available_date = None
        floor = None
        deposit = None
        balcony = None
        washing_machine = None
        dishwasher = None
        elevator = None
        parking = None
        pets_allowed = None
        external_id = None
        
        external_id_info = response.css('head > link').extract()
        for i in range(len(external_id_info)):
            if '<link rel="shortlink" href="https://sleepwellmanagement.com/?p=' in external_id_info[i]:
                external_id=external_id_info[i]
        external_id = external_id.split("?p=")[1].split('\"')[0]

        title = response.css("h2::text").get()
        description = response.css("div.mkdf-property-description.mkdf-property-label-items-holder > div.mkdf-property-description-items.mkdf-property-items-style.clearfix > p::text").extract()
        
        address = response.css("div.mkdf-property-map-address > div > div > span > span.mkdf-label-items-value::text").get()
        city = address.split(", ")[1].split(",")[0]

        rent = response.css("span.mkdf-property-price-value::text").get()
        if "," in rent:
            rent = rent.replace(",","")
        rent = int(rent)
        try:
            deposit = response.css("div:nth-child(4) > div.mkdf-property-spec-items.mkdf-property-items-style.clearfix > div > div > div:nth-child(2) > div > span.mkdf-spec-item-value.mkdf-label-items-value::text").get()
            if "," in deposit:
                deposit =int(deposit.replace(",",""))
            if "Required" in deposit:
                deposit = None
        except:
            pass
        room_count = None
        try:
            room_count = int(response.css("span.mkdf-property-value::text").get())
        except:
            pass
        if room_count is None:
            room_count = 1
        bathroom_count = int(math.floor(float(response.css("div:nth-child(3) > span.mkdf-property-content > span.mkdf-property-value::text").get().strip())))
        try:
            square_meters = int(math.ceil(int(response.css("div:nth-child(4) > span.mkdf-property-content > span.mkdf-property-value > span.mkdf-property-size-value::text").get())/10.764))
        except:
            pass
        
        images = response.css(".mkdf-property-single-lightbox img::attr(src)").extract()
        external_images_count = len(images)

        try:
            available_date = response.css("div:nth-child(5) > span.mkdf-property-content > span.mkdf-property-value::text").get()
            month = available_date.split(" ")[1].split(" ")[0]
            year = available_date.split(", ")[1]
            day = available_date.split(" ")[1].split(" ")[1].split(",")[0][0]
            available_date = year+'-'+month+'-'+day
        except:
            pass
        if "Flexible" in available_date or available_date == " ":
                available_date = None 
        try:
            floor = response.css("div:nth-child(2) > div.mkdf-property-spec-items.mkdf-property-items-style.clearfix > div > div > div:nth-child(4) > div > span.mkdf-spec-item-value.mkdf-label-items-value::text").get()
        except:
            pass
        info = response.css(".mkdf-feature-active > span::text").extract()
        if " Pet Friendly " in info:
            pets_allowed = True
        if " Outdoor Parking " in info or " Covered Parking " in info:
            parking = True
        if " Elevator " in info: 
            elevator = True
        if " Balcony " in info:
            balcony = True
        if " Laundry Facilities " in info or " Ensuite Laundry " in info:
            washing_machine = True
        if " Dishwasher " in info:
            dishwasher = True
        
        property_type = 'house'

        item_loader.add_value('external_link', response.url)
        item_loader.add_value('external_id',external_id)        
        item_loader.add_value('external_source', self.external_source)
        item_loader.add_value('title',title)
        item_loader.add_value('description',description)
        item_loader.add_value('city',city)
        item_loader.add_value('address',address)
        item_loader.add_value('property_type',property_type)
        item_loader.add_value('square_meters',square_meters)

        item_loader.add_value('room_count',room_count)
        item_loader.add_value('bathroom_count',bathroom_count)
        item_loader.add_value('available_date',available_date)
        item_loader.add_value('images',images)
        item_loader.add_value('external_images_count',external_images_count)
        item_loader.add_value('rent',rent)
        item_loader.add_value('currency','CAD')
        item_loader.add_value('deposit',deposit)
        item_loader.add_value('pets_allowed',pets_allowed)
        item_loader.add_value('floor',floor)
        item_loader.add_value('parking',parking)
        item_loader.add_value('elevator',elevator)
        item_loader.add_value('balcony',balcony)
        item_loader.add_value('washing_machine',washing_machine)
        item_loader.add_value('dishwasher',dishwasher)
        item_loader.add_value("landlord_name", "sleepwellmanagement")
        item_loader.add_value("landlord_phone", "+1-877-521-2004")
        item_loader.add_value("landlord_email", "info@sleepwellmanagement.com")
        yield item_loader.load_item()
