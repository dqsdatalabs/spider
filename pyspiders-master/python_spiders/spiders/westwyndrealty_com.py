import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader


class WestwyndrealtyComSpider(scrapy.Spider):
    name = 'westwyndrealty_com'
    allowed_domains = ['westwyndrealty.com']
    start_urls = [
        'https://westwyndrealty.com/rental-search?status=for_rent&property_type=&city=&bedrooms=&bathrooms=&pets=&searching=true']
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'

    def parse(self, response):
        for appartment in response.css("#wrapper > div.container > div > div.col-md-8 > div.listings-container.list-layout > div"):
            url = appartment.css("a").attrib['href']
            yield Request(url,
                          callback=self.populate_item,
                          )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.css(
            '#titlebar > div > div > div > div.property-title > h2::text').get()

        address = response.css(
            '#titlebar > div > div > div > div.property-title > span > a::text').extract()[1]
        city = address.strip().split(', ')[1]
        zipcode = address.strip().split(', ')[0].split(' ')[0]

        feats = response.css(
            '#wrapper > div:nth-child(5) > div > div.col-lg-8.col-md-7 > div > ul.property-main-features > li')

        bedrooms = None
        bathrooms = None
        pets = None
        space = None
        for item in feats:
            if "Bathrooms" in item.css("::text").get():
                bathrooms = item.css('span::text').get()
            elif "Bedrooms" in item.css("::text").get():
                bedrooms = item.css('span::text').get()
            elif "Pets" in item.css("::text").get():
                pets = item.css('span::text').get().strip()
                try:
                    if "Yes" in pets:
                        pets = True
                    else:
                        pets = False
                except:
                    pets = None
            elif "Area" in item.css("::text").get():
                space = item.css('span::text').get().strip().split(" ")[0]
                if "," in space:
                    space = space.split(",")
                    space = space[0]+space[1]
                space = int(space)/10.7639

        description = ''
        description_array = response.css(
            "div.show-more::text").extract()

        rent = response.css(
            '#titlebar > div > div > div > div.property-pricing > div:nth-child(1)::text').get()
        rent = rent.split("$")[1].split(' ')[0]

        if ',' in rent:
            rent = rent.replace(',', '')

        for item in description_array:
            description += item

        images = response.css(
            'div.item > img::attr(src)').extract()

        # for i in range(len(images)):
        #     images[i] = images[i].split(':url(')[1].split(")")[0]

        # coords = response.xpath(
        #     '//*[@id="all_wrapper"]/div/script[18]/text()').get()
        # lat = coords.split('general_latitude":"')[1].split('",')[0]
        # lng = coords.split('longitude":"')[1].split('",')[0]

        # landlord_name = response.css(
        #     '#primary > div > div.agent_unit > div:nth-child(2) > h4 > a::text').get()
        # landlord_number = response.css(
        #     '#primary > div > div.agent_unit > div:nth-child(2) > div:nth-child(3)::text').get()
        # landlord_email = response.css(
        #     '#primary > div > div.agent_unit > div:nth-child(2) > div:nth-child(4)::text').get()

        # MetaData
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        # item_loader.add_value(
        #     "external_id", "{}".format(id.split("=")[-1].strip()))
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)

        # Property Details
        item_loader.add_value("property_type", "apartment")
        item_loader.add_value("square_meters", int(int(int(space))*10.764))
        item_loader.add_value("room_count", bedrooms)
        item_loader.add_value("bathroom_count", bathrooms)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("pets_allowed", pets)

        # item_loader.add_value("available_date", avaialble_date)
        # item_loader.add_value("parking", parking)
        # item_loader.add_value("swimming_pool", pool)
        # item_loader.add_value("balcony", balcony)

        # item_loader.add_value("latitude", lat)
        # item_loader.add_value("longitude", lng)

        # Images
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        # # Monetary Status
        item_loader.add_value("rent", int(rent))
        item_loader.add_value("currency", "CAD")

        # LandLord Details
        item_loader.add_value("landlord_phone", '(604) 944-8917')
        item_loader.add_value("landlord_email", 'inquiries@westwyndrealty.com')
        item_loader.add_value("landlord_name", 'Westwynd Reality')

        yield item_loader.load_item()
