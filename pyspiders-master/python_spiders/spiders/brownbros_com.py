import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader


class BrownbrosComSpider(scrapy.Spider):
    name = 'brownbros_com'
    allowed_domains = ['brownbros.com']
    start_urls = ['https://brownbros.com/property/residential']
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    def parse(self, response):
        for appartment in response.css("div.property-list > a"):
            url = "https://brownbros.com/" + appartment.css("a").attrib['href']
            yield Request(url,
                          callback=self.populate_item,
                          )
    # 2. SCRAPING level 2

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.css(
            'body > div.l-page > section > div > h1::text').get()

        rent = response.css(
            'div.condo__details.condo__details--1 > div.price::text').extract()[1].strip().split("$")[1]

        if "," in rent:
            rent = rent.split(",")[0] + rent.split(",")[1]

        if "." in rent:
            rent = rent.split(".")[0]

        description = ""
        description_array = response.css(
            "body > div.l-page > main > article > div.condo__details.condo__details--about > div > p::text").extract()

        for item in description_array:
            description += item

        images = response.css(
            'div.swiper-slide > img::attr(src)').extract()

        for i in range(len(images)):
            images[i] = "https://brownbros.com/" + images[i]

        try:
            lat = response.css('#property-map::attr(data-lat)').get()
            lng = response.css('#property-map::attr(data-lng)').get()
        except:
            pass

        available_date = response.css(
            'div.available::text').get().split('Available')[1].strip()

        features = response.css(
            'ul.condo__details.condo__details--list > li')

        furnished = None
        pets = None
        deposit = None
        for item in features:
            if "Pet-Friendly" in item.css('span.label::text').get():
                pets = item.css('span:nth-child(2)::text').get()
                if "No" in pets:
                    pets = False
                else:
                    pets = True
            elif "Furnished" in item.css('span.label::text').get():
                furnished = item.css('span:nth-child(2)::text').get()
                if "No" in furnished:
                    furnished = False
                else:
                    furnished = True
            elif "Security Deposit" in item.css('span.label::text').get():
                deposit = item.css(
                    'span:nth-child(2)::text').get().split("$")[1]
                if "." in deposit:
                    deposit = deposit.split(".")[0]
                deposit = deposit.strip()

        house_feats = response.css(
            'ul.condo__details.condo__details--boxes > li > span::text').extract()
        rooms = house_feats[1]
        bathrooms = house_feats[2]
        try:
            space = house_feats[3].strip().split(' ')[-1]
            if "+" in space:
                space = space.split('+')[0]

            space = int(int(space.split('*')[1].strip()) * 0.0929)
        except:
            space = house_feats[4].strip().split(' ')[-1]
            if "+" in space:
                space = space.split('+')[0]

            space = int(int(space.split('*')[1].strip()) * 0.0929)

        # property_type = house_feats[0]
        # if 'condo' in property_type:
        #     property_type = "apartment"

        # MetaData
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)

        # Property Details
        item_loader.add_value("property_type", "apartment")
        item_loader.add_value("room_count", rooms)
        item_loader.add_value("bathroom_count", bathrooms)
        item_loader.add_value("address", title)
        item_loader.add_value("square_meters", int(int(space)*10.764))
        item_loader.add_value("available_date", available_date)

        item_loader.add_value("furnished", furnished)
        item_loader.add_value("pets_allowed", pets)

        item_loader.add_value("latitude", lat)
        item_loader.add_value("longitude", lng)

        # item_loader.add_value("energy_label", energy)

        # Images
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        # Monetary Status
        item_loader.add_value("rent", int(rent))
        item_loader.add_value("deposit", int(deposit))
        item_loader.add_value("currency", "CAD")

        # LandLord Details
        item_loader.add_value("landlord_phone", "250.385.8771")
        item_loader.add_value("landlord_email", "info@brownbros.com")
        item_loader.add_value("landlord_name", "Brown Bros.")

        yield item_loader.load_item()
