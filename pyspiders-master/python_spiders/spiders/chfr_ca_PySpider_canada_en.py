import scrapy
from scrapy import Request
from ..loaders import ListingLoader

class chfr_ca_PySpider_canadaSpider(scrapy.Spider):
    name = 'chfr_ca'
    allowed_domains = ['chfr.ca']
    start_urls = [
        'https://chfr.ca/?s=&qodef-property-search=yes&qodef-search-type=&qodef-search-city=&qodef-search-status=47'
        ]
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    def parse(self, response):  #page_follower
        urls = response.css(".qodef-block-drag-link::attr(href)").extract()
        for url in urls:
            yield Request(url=url,
            callback = self.parse_property)

    def parse_property(self, response):
        item_loader = ListingLoader(response=response)
        room_count = None
        dishwasher = None
        elevator = None
        furnished = None
        parking = None
        washing_machine = None
        balcony = None
        swimming_pool = None
        external_id = None
        zipcode = None
        address = None
        city = None

        external_id_info = response.css('head > link').extract()
        for i in range(len(external_id_info)):
            if '<link rel="shortlink" href="https://chfr.ca/?p=' in external_id_info[i]:
                external_id=external_id_info[i]
        external_id = external_id.split("?p=")[1].split('\"')[0]

        title = response.css("div.qodef-property-title-cell.qodef-title-left > h2::text").get()
        
        description_info = response.css("div.qodef-property-description.qodef-property-label-items-holder > div.qodef-property-description-items.qodef-property-items-style.clearfix > p").extract()
        description = ''
        for i in range(len(description_info)):
            description = description + description_info[i]
        description = description.split("References Required")[0]
        description = description.split("For more information")[0]

        rent = response.css("span.qodef-property-price-label::text").get()
        rent = rent.split(" ")[0].replace("$","")
        if "/Month" in rent:
            rent = rent.replace("/Month","")
        if ".00" in rent:
            rent = rent.replace(".00","")
        if "," in rent:
            rent = rent.replace(",","")
        rent = int(rent)
        images = response.css(".qodef-property-single-lightbox img::attr(src)").extract()
        external_images_count = len(images)
    
        try:
            address = response.css(".qodef-full-address .qodef-label-items-value::text").get().strip()
            city = address.split(", ")[1].split(",")[0]
            zipcode = address.split(", Canada")[0].split(",")[-1].replace("NB","").strip()
            if zipcode == "":
                zipcode = None
        except:
            pass
        
        try:
            room_count = response.css("div:nth-child(1) > div > span.qodef-spec-item-value.qodef-label-items-value::text").get().strip()
            if '.5' in room_count:
                room_count = room_count.replace(".5","")
            if 'Studio' in room_count:
                room_count = 1
            room_count = int(room_count)
        except:
            room_count = None
            pass
        bathroom_count = response.css("div:nth-child(2) > div > span.qodef-spec-item-value.qodef-label-items-value::text").get().strip()
        try:
            bathroom_count = int(bathroom_count)
        except:
            pass

        
        features = response.css(".qodef-feature-active > span::text").extract()
        for i in range(len(features)):
            features[i]=features[i].strip()
        if "Dishwasher" in features:
            dishwasher = True
        if "Elevator in Building" in features:
            elevator = True
        if "Furnished" in features:
            furnished = True
        if "Garage" in features or "Off-Street Parking" in features:
            parking = True
        if "In Suite Laundry" in features or "On Site Laundry" in features or "Washer/Dryer Hookups" in features:
            washing_machine = True
        if  "Private Balcony" in features:
            balcony = True
        if "Swimming Pool" in features:
            swimming_pool = True
        property_type = 'apartment'
        
        counter = None
        inf = response.css(".qodef-leasing-term .qodef-label-text::text").extract()
        for i in range(len(inf)):
            inf[i] = inf[i].strip()
            if "Available:" in inf[i]:
                counter = i
        inf_value = response.css(".qodef-leasing-value::text").extract()
        for i in range(len(inf_value)):
            inf_value[i] = inf_value[i].strip()
        available_date = None
        if "Available:" in inf:
            available_date = inf_value[counter]

        if room_count is not None:
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
            item_loader.add_value('property_type', property_type)
            item_loader.add_value('room_count',room_count)
            item_loader.add_value('bathroom_count',bathroom_count)
            item_loader.add_value('available_date',available_date)
            item_loader.add_value('images',images)
            item_loader.add_value('external_images_count',external_images_count)
            item_loader.add_value('rent',rent)
            item_loader.add_value('currency',"CAD")
            item_loader.add_value('furnished',furnished)
            item_loader.add_value('parking',parking)
            item_loader.add_value('elevator',elevator)
            item_loader.add_value('balcony',balcony)
            item_loader.add_value('swimming_pool',swimming_pool)
            item_loader.add_value('washing_machine',washing_machine)
            item_loader.add_value('dishwasher',dishwasher)
            item_loader.add_value("landlord_name", "chfr.ca")
            item_loader.add_value("landlord_phone", "506-216-3113")
            item_loader.add_value("landlord_email", "info@canadahomesforrent.ca")
            yield item_loader.load_item()
