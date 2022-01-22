import scrapy
from scrapy import Request
from ..loaders import ListingLoader

class pierrecarapetian_PySpider_canadaSpider(scrapy.Spider):
    name = 'pierrecarapetian_com'
    allowed_domains = ['pierrecarapetian.com']
    start_urls = [ 
        'https://pierrecarapetian.idxbroker.com/idx/details/listing/a126/W5405867/530-Indian-Grve-610-Toronto-ON-M6P-2J2?widgetReferer=true',
        'https://pierrecarapetian.idxbroker.com/idx/details/listing/a126/W5405850/530-Indian-Grve-610-Toronto-ON-M6P-0B3?widgetReferer=true',
        'https://pierrecarapetian.idxbroker.com/idx/details/listing/a126/W5406084/816-Lansdowne-Ave-103-Toronto-ON-M6H-4K6?widgetReferer=true',
        'https://pierrecarapetian.idxbroker.com/idx/details/listing/a126/C5406015/38-Joe-Shuster-Way-1223-Toronto-ON-M6K-0A5?widgetReferer=true',
        'https://pierrecarapetian.idxbroker.com/idx/details/listing/a126/C5405926/21-Widmer-St-1605-Toronto-ON-M5V-0B8?widgetReferer=true',
        'https://pierrecarapetian.idxbroker.com/idx/details/listing/a126/C5405926/21-Widmer-St-1605-Toronto-ON-M5V-0B8?widgetReferer=true',
        'https://pierrecarapetian.idxbroker.com/idx/details/listing/a126/C5406012/390-Cherry-St-2908-Toronto-ON-M5A-0E2?widgetReferer=true',
        'https://pierrecarapetian.idxbroker.com/idx/details/listing/a126/C5406010/21-Iceboat-Terr-2709-Toronto-ON-M5V-4A9?widgetReferer=true',
        'https://pierrecarapetian.idxbroker.com/idx/details/listing/a126/C5405935/15-Fort-York-Blvd-3707-Toronto-ON-M5V-3Y4?widgetReferer=true',
        'https://pierrecarapetian.idxbroker.com/idx/details/listing/a126/C5405905/501-Yonge-St-3010-Toronto-ON-M4Y-1Y4?widgetReferer=true',
        'https://pierrecarapetian.idxbroker.com/idx/details/listing/a126/C5406106/18-Harbour-St-2902-Toronto-ON-M5J-2Z6?widgetReferer=true',
        'https://pierrecarapetian.idxbroker.com/idx/details/listing/a126/C5406001/70-Temperance-St-611-Toronto-ON-M5H-0B1?widgetReferer=true',
        'https://pierrecarapetian.idxbroker.com/idx/details/listing/a126/C5406135/155-Yorkville-Ave-1418-Toronto-ON-M5R-1C4?widgetReferer=true',
        'https://pierrecarapetian.idxbroker.com/idx/details/listing/a126/C5406105/403-Church-St-305-Toronto-ON-M4Y-2C2?widgetReferer=true',
        'https://pierrecarapetian.idxbroker.com/idx/details/listing/a126/C5405965/1-Shaw-St-729-Toronto-ON-M6K-0A1?widgetReferer=true',
        'https://pierrecarapetian.idxbroker.com/idx/details/listing/a126/C5405941/25-Telegram-Mews-811-Toronto-ON-M5V-3Z1?widgetReferer=true',
        'https://pierrecarapetian.idxbroker.com/idx/details/listing/a126/C5405866/12-York-St-2004-Toronto-ON-M5J-2Z2?widgetReferer=true',
        'https://pierrecarapetian.idxbroker.com/idx/details/listing/a126/C5406052/555-Yonge-St-903-Toronto-ON-M4Y-3A6?widgetReferer=true',
        'https://pierrecarapetian.idxbroker.com/idx/details/listing/a126/C5406133/33-E-Charles-St-1204-Toronto-ON-M4Y-0A2?widgetReferer=true'

    ]
    country = 'canada'
    locale = 'en_ca'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    def parse(self, response):
        item_loader = ListingLoader(response=response)
        title = response.css("#IDX-field-address > span::text").get()
        # if "Parking" not in title:
        external_id = response.css("#IDX-field-listingID > span::text").get().strip()
        room_count = None
        try:
            room_count = int(response.css("#IDX-field-bedrooms > span::text").get())
        except:
            pass
        bathroom_count = int(response.css("#IDX-field-totalBaths > span::text").get())
        square_meters = response.css("#IDX-field-approxSquareFootage > span::text").get().split('-')[1]
        square_meters = round(int(square_meters)/10.764,1)
        property_type = response.css("#IDX-field-style > span::text").get().lower().strip()
        balcony = response.css("#IDX-field-balcony > span::text").get()
        if balcony is not None:
            balcony = True
        else:
            balcony = False
        parking = response.css("#IDX-field-parkingSpaces > span::text").get()
        if parking is not None:
            parking = True
        else:
            parking = False
        description = response.css("#IDX-field-remarksConcat > span::text").get()
        swimming_pool = None
        dishwasher = None
        washing_machine = None
        pets_allowed = None
        furnished = None
        try:
            extra_info = response.css("#IDX-field-extras > span::text").get()
            if "Swimming Pool" in extra_info:
                swimming_pool = True
            if "Dishwasher" in extra_info:
                dishwasher = True
            if "Washer" in extra_info:
                washing_machine = True
            if "No Pets"in extra_info:
                pets_allowed = False
            if "Furnished" in extra_info:
                Furnished = True
        except:
            pass
        images = response.css("#IDX-detailsShowcase > div > div > img::attr(src)").extract()
        external_images_count = len(images)
        rent = response.css("#IDX-field-listingPrice > span::text").get()
        if (',') in rent:
            rent = int(rent.replace(",","").replace("$",""))
        else:
            rent = int(rent.replace("$",""))
        currency = "CAD"
        city = response.css("#IDX-field-cityName > span::text").get().strip()
        zipcode = response.css("#IDX-field-zipcode > span::text").get().strip()
        address = (title,", ",city,",",zipcode)
        latitude = response.css("#IDX-map::attr(data-lat)").get()
        longitude = response.css("#IDX-map::attr(data-lng)").get()

        item_loader.add_value('external_link', response.url)
        item_loader.add_value('external_id', external_id)
        item_loader.add_value('external_source', self.external_source)
        item_loader.add_value('title', title)
        item_loader.add_value('description', description)
        item_loader.add_value('city',city)
        item_loader.add_value('zipcode',zipcode)
        item_loader.add_value('address', address)
        item_loader.add_value('latitude',latitude)
        item_loader.add_value('longitude',longitude)
        item_loader.add_value('property_type', property_type)
        item_loader.add_value('square_meters', int(int(square_meters)*10.764))
        item_loader.add_value('room_count',room_count)
        item_loader.add_value('bathroom_count',bathroom_count)
        item_loader.add_value('images',images)
        item_loader.add_value('external_images_count',external_images_count)
        item_loader.add_value('rent',rent)
        item_loader.add_value('currency',currency)
        item_loader.add_value('pets_allowed',pets_allowed)
        item_loader.add_value('furnished',furnished)
        item_loader.add_value('parking',parking)
        item_loader.add_value('balcony', balcony)
        item_loader.add_value('washing_machine',washing_machine)
        item_loader.add_value('dishwasher',dishwasher)
        item_loader.add_value('landlord_name','pierrecarapetian')
        item_loader.add_value('landlord_email','pierre@pierrecarapetian.com')
        item_loader.add_value('landlord_phone','416 424 3434')
        yield item_loader.load_item()