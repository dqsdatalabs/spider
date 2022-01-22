import scrapy
from ..loaders import ListingLoader
from ..helper import *
from scrapy.http import HtmlResponse
import json


class LuewoDeSpider(scrapy.Spider):
    name = "luewo_de"
    start_urls = ['https://luewo.de/wohnungen/']
    allowed_domains = ["luewo.de"]
    country = 'germany'  # Fill in the Country's name
    locale = 'de'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        urls = [i for i in response.xpath('//*[@id="immoResult"]/div[*]/a/@href').extract() if 'http' in i]
        for url in urls:
            yield scrapy.Request(url, callback=self.populate_item)

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.css('.hide-for-small-only::text').get().strip()
        loc = response.css('.large-6:nth-child(1)::text').get().strip()
        longitude, latitude = extract_location_from_address(loc)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        description = ' '.join(response.xpath('//*[@id="description"]/div[1]/text()').extract()).strip() + ' '.join(
            response.xpath('//*[@id="location"]/div/text()').extract()).strip()

        images = [re.search(r'http(.*).jpg', i)[0] for i in response.css('img::attr(data-src)').extract() if
                  '.jpg' in i and 'luewo-team' not in i]

        room_count = int(float(response.css('#keyFacts strong::text').extract()[1].split()[0]))
        rent = int(float(response.css('#keyFacts strong::text').extract()[2].split()[0]))
        square_meter = int(float(response.css('#keyFacts strong::text').extract()[0].split()[0]))

        details = response.css('dd::text').extract()
        pets_allowed = furnished = parking = elevator = balcony = terrace = swimming_pool = washing_machine \
            = dishwasher = None
        pets_allowed, furnished, parking, elevator, balcony, terrace, swimming_pool, washing_machine, \
        dishwasher = get_amenities(description, ' '.join(details), item_loader)

        info = dict(zip(response.css('.xlarge-up-4 strong::text').extract(),
                        [i.strip() for i in response.xpath('//*[@id="main"]/section[3]/ul/li[*]/text()').extract() if
                         i.strip() != '']))

        utilities = deposit = available_date = heating_cost = None
        bathroom_count = 1
        for i in info.keys():
            if 'kaution' in i.lower():
                deposit = int(float(info[i].split()[0])) * rent
            if 'balkon' in i.lower():
                balcony = True
            if 'bezugsfertig' in i.lower():
                if '.' in info[i]:
                    available_date = '-'.join(info[i].split('.')[::-1])
            if 'heizkosten' in i.lower():
                heating_cost = int(float(info[i].split()[0]))
            if 'nebenkosten' in i.lower():
                utilities = int(float(info[i].split()[0]))




        description = description_cleaner(description)

        # Enforces rent between 0 and 40,000 please dont delete these lines
        if 0 >= int(rent) > 40000:
            return

        # # MetaData
        item_loader.add_value("external_link", response.url)  # String
        item_loader.add_value("external_source", self.external_source)  # String

        # item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position)  # Int
        item_loader.add_value("title", title)  # String
        item_loader.add_value("description", description)  # String

        # # Property Details
        item_loader.add_value("city", city)  # String
        item_loader.add_value("zipcode", zipcode)  # String
        item_loader.add_value("address", address)  # String
        item_loader.add_value("latitude", latitude)  # String
        item_loader.add_value("longitude", longitude)  # String
        # item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type",
                              'apartment')  # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meter)  # Int
        item_loader.add_value("room_count", room_count)  # Int
        item_loader.add_value("bathroom_count", bathroom_count)  # Int

        item_loader.add_value("available_date", available_date)  # String => date_format

        item_loader.add_value("pets_allowed", pets_allowed)  # Boolean
        item_loader.add_value("furnished", furnished)  # Boolean
        item_loader.add_value("parking", parking)  # Boolean
        item_loader.add_value("elevator", elevator)  # Boolean
        item_loader.add_value("balcony", balcony)  # Boolean
        item_loader.add_value("terrace", terrace)  # Boolean
        item_loader.add_value("swimming_pool", swimming_pool)  # Boolean
        item_loader.add_value("washing_machine", washing_machine)  # Boolean
        item_loader.add_value("dishwasher", dishwasher)  # Boolean

        # # Images
        item_loader.add_value("images", images)  # Array
        item_loader.add_value("external_images_count", len(images))  # Int
        # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent)  # Int
        item_loader.add_value("deposit", deposit)  # Int
        # item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities)  # Int
        item_loader.add_value("currency", "EUR")  # String

        # item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heating_cost)  # Int

        # item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", 'Lüdenscheider Wohnstätten')  # String
        item_loader.add_value("landlord_phone", '0 23 51/18 95-55')  # String
        item_loader.add_value("landlord_email", 'mail@luewo.de')  # String

        self.position += 1
        yield item_loader.load_item()
