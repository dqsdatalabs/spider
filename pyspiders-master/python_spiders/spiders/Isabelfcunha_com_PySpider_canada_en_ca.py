import scrapy
from scrapy import Request
from ..loaders import ListingLoader

class isabelfcunha_PySpider_canadaSpider(scrapy.Spider):
    name = 'isabelfcunha_com'
    allowed_domains = ['isabelfcunha.com']
    start_urls = [
        'https://isabelfcunha.com/toronto-condos-for-rent',
        'https://isabelfcunha.com/liberty-village-condos-for-rent',
        'https://isabelfcunha.com/fort-york-condos-for-rent',
        'https://isabelfcunha.com/yorkville-condos-for-rent'
        ]
    country = 'canada'
    locale = 'en_ca'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    def parse(self, response):
        urls = response.css("#loadmoremobile_1 a::attr(href)").extract()
        for url in urls:
            url = "https://isabelfcunha.com/" + url
            yield Request(url=url,
            callback = self.parse_property)

    def parse_property(self, response):
        item_loader = ListingLoader(response=response)
        title = response.css("#main > div.row.sect > div:nth-child(1) > div.row.lstd > div > div > div.lstInfo > h2::text").get()
        external_id = response.css("#main > div.row.sect > div:nth-child(1) > div.row.lstd > div > div > div.lstInfo > p:nth-child(4)").get()
        try:
            external_id = external_id.split("#:")[1].split("\n")[1].strip()
        except:
            pass
        rent = response.css("#main > div.row.sect > div:nth-child(1) > div.row.lstd > div > div > div.hPrice > span::text").get().replace("$","")
        if "," in rent:
            rent = rent.replace(",","")
        if "Weekly" in rent:
            rent = int(rent.split("/")[0])*4
        else:
            rent = int(rent.split("/")[0])
        currency = "CAD"
        city = response.css("#listingInfo > div > div.listInfo > div:nth-child(1) > ul > li:nth-child(2) > span.b::text").get()
        property_type = response.css("#listingInfo > div > div.listInfo > div:nth-child(2) > ul > li:nth-child(2) > span.b::text").get()
        if 'Apt' in property_type:
            property_type = 'apartment'
        else:
            property_type = 'house'
        parking = None
        furnished = None
        swimming_pool = None
        elevator = None
        info = response.css("#listingInfo > div > div.listInfo").get()
        if "Beds" in info:
            room_count = info.split("Beds:</span><span class=\"b\">")[1].split("</span>")[0]
            if "+" in room_count:
                r1 = int(room_count.split('+')[0])
                r2 = int(room_count.split('+')[1])
                room_count = r1+r2
            else:
                room_count = int(room_count)
        else:
            room_count = 1 
        bathroom_count = int(info.split("Bath:</span><span class=\"b\">")[1].split("</span>")[0])
        square_meters = round(int(info.split("Size:</span><span class=\"b\">")[1].split("</span>")[0].replace("Sq Ft","").split("-")[1])/10.764,1)
        description = response.css("#listingInfo > div > div.listingDesc > p:nth-child(3)::text").get()
        info2 = response.css("#listingInfo > div > div.listingDesc").get()
        washing_machine = None
        dishwasher = None
        if "Dishwasher" in info2:
            dishwasher = True
        if "Washer" in info2:
            washing_machine = True
        if 'Pool' in info:
            swimming_pool = True
        if 'elevator' in info or info2:
            elevator = True
        if "Furnished" in info:
            furnished = True
        if "Parking" or "Garage" in info:
            parking = True
        address = title+','+city
        images = response.css("img::attr(src)").extract()
        images = images[0]
        external_images_count = 1
        

        item_loader.add_value('external_link', response.url)
        item_loader.add_value('external_id', external_id)
        item_loader.add_value('external_source', self.external_source)
        item_loader.add_value('title', title)
        item_loader.add_value('description', description)
        item_loader.add_value('city',city)
        item_loader.add_value('address', address)
        item_loader.add_value('property_type', property_type)
        item_loader.add_value('square_meters', int(int(square_meters)*10.764))
        item_loader.add_value('room_count',room_count)
        item_loader.add_value('bathroom_count',bathroom_count)
        item_loader.add_value('images',images)
        item_loader.add_value('external_images_count',external_images_count)
        item_loader.add_value('rent',rent)
        item_loader.add_value('currency',currency)
        item_loader.add_value('furnished',furnished)
        item_loader.add_value('parking',parking)
        item_loader.add_value('elevator',elevator)
        item_loader.add_value('swimming_pool',swimming_pool)
        item_loader.add_value('washing_machine',washing_machine)
        item_loader.add_value('dishwasher',dishwasher)
        item_loader.add_value('landlord_name','isabelfcunha')
        item_loader.add_value('landlord_email','(416) 358-4078')
        item_loader.add_value('landlord_phone','(416) 966-0300')
        yield item_loader.load_item()