import scrapy
from scrapy import Request
from ..loaders import ListingLoader


class devonproperties_PySpider_canadaSpider(scrapy.Spider):
    name = 'devonproperties_com'
    allowed_domains = ['devonproperties.com']
    start_urls = [
        'https://devonproperties.com/properties/residential/'
        ]
    country = 'canada'
    locale = 'en_ca'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    def parse(self, response):
        urls = response.css("body > div.l-page > main > div > div.property-archive__listings.archive__content.fixbox__content > div.posts.post-list> div > a::attr(href)").extract()
        print(urls)
        for url in urls:
            yield Request(url=url,
            callback = self.parse_property)

    def parse_property(self, response):
        property_listings = int(response.css("#printableArea > div.inner > div.property__listings > div > span.property__listings__count::text").get())
        
        counter = 1 
        while counter <= property_listings:
            item_loader = ListingLoader(response=response)
            listings_info = response.css("#printableArea > div.inner > div.property__listings > ul > li:nth-child("+str(counter)+")").get() 
            listing_amenities = response.css("#printableArea > div.inner > div.property__listings > ul > li:nth-child("+str(counter)+") > div.property__listing__details > div.property__listing__amenities.accordion.accordion--small > div > div.accordion__content > ul").get()
            balcony = None
            dishwasher = None
            square_meters = None
            try:
                if "Balcony" in listing_amenities:
                    balcony = True
            except:
                pass
            try:
                if "Dishwasher" in listing_amenities:
                    dishwasher = True
            except:
                pass
            if "Bedroom" in listings_info:
                room_count = listings_info.split('icon-bed"></use></svg> ')[1].split(' Bedroom')[0]
                if "Junior" in room_count:
                    room_count = int(room_count.split("Junior")[1])
            if "Bachelor" in listings_info:
                room_count = 1
            bathroom_count = listings_info.split('icon-bath"></use></svg> ')[1].split(' Bath')[0]
            if ".5" in bathroom_count:
                bathroom_count = int(bathroom_count.replace(".5",""))
                bathroom_count = bathroom_count + 1
            else:
                bathroom_count = int(bathroom_count)
            try:
                square_meters = round(int(listings_info.split('sqft"></use></svg> ')[1].split(' sq.ft.')[0])/10.764,1)
            except:
                pass
            rent = int(listings_info.split('price">From <span>$')[1].split(' / mo')[0])
            available_date = listings_info.split('availability"><span class="label">Available </span><span>')[1].split('</span></div>')[0]
            counter = counter + 1

            title = response.css("#printableArea > div.inner > header > h1::text").get()
            property_type = response.css("#printableArea > div.inner > div.property__data > div.property__building-type > span::text").get()
            if "Apartment" in property_type:
                property_type = 'apartment'
            else:
                property_type = 'house'
            pets_info = response.css("#printableArea > div.inner > div.property__data > div.property__pets > span::text").get()
            pets_allowed = None
            if "Pet Friendly" in pets_info:
                pets_allowed = True
            address = response.css("#printableArea > div.inner > div.property__location > address").get().split('<address>')[1].split('</address>')[0].replace("<br>",",")       
            city = address.split(",")[1].split(",")[0].strip()
            description = response.css("head > meta:nth-child(16)").get().split('content=')[1]
            images = response.css("body > div.l-page > main > article > section.property__gallery.slider.fixbox__pin > div > div > div.swiper-wrapper > div > a::attr(href)").extract()
            for i in range(len(images)):
                images[i] = "https://devonproperties.com/" + images[i]
            external_images_count = len(images)
            
            property_amenities = response.css("#printableArea > div.inner > div.property__details.accordion > div > div.property__amenities.accordion__container > div.accordion__content > ul").get() 
            parking = None
            elevator = None
            swimming_pool = None
            washing_machine = None
            terrace = None
            if 'Elevator' in property_amenities:
                elevator = True
            
            if "Parking" in property_amenities:
                parking = True
            if "Pool" in property_amenities:
                swimming_pool = True
            if "Laundry" in property_amenities:
                washing_machine = True
            if "Terrace" in property_amenities:
                terrace = True
            

            item_loader.add_value('external_link', response.url)        
            item_loader.add_value('external_source', self.external_source)
            item_loader.add_value('title',title)
            item_loader.add_value('description',description)
            item_loader.add_value('city',city)
            item_loader.add_value('address',address)
            item_loader.add_value('property_type',property_type)
            item_loader.add_value('square_meters',int(int(square_meters)*10.764))
            item_loader.add_value('room_count',room_count)
            item_loader.add_value('bathroom_count',bathroom_count)
            item_loader.add_value('available_date',available_date)
            item_loader.add_value('images',images)
            item_loader.add_value('external_images_count',external_images_count)
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
            item_loader.add_value("landlord_phone", "250.883.6263")
            item_loader.add_value("landlord_email", "mbryan@devonproperties.com")
            item_loader.add_value("landlord_name", "Mitch Bryan (Leasing Specialist)")
            yield item_loader.load_item()
