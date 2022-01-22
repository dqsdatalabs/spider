# -*- coding: utf-8 -*-
# Author: Fill with the developer's Name
import scrapy
from scrapy import Request
from ..loaders import ListingLoader
from ..helper import extract_location_from_address, extract_location_from_coordinates


class PuntocasaPyspiderItalySpider(scrapy.Spider):
    name = "puntocasa"
    start_urls = ['https://www.puntocasa.net/it/residenziale/Affitto']
    allowed_domains = ["puntocasa.net"]
    execution_type = 'testing'
    country = 'italy'  # Fill in the Country's name
    locale = 'it'  # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        # Your code goes here
        apartments_pages = []
        apartment_divs = response.xpath('.//div[contains(@class, "box_lista")]')

        for apartment_div in apartment_divs:
            landlord_phone = apartment_div.xpath('.//a[contains(@class, "btn-view-tel")]/@data-tel').extract()
            apartments_pages.append({
                "url": apartment_div.xpath('.//a[contains(@class, "link_nero")]/@href')[0].extract(),
                "landlord_phone": landlord_phone
            })

        for apartment_page in apartments_pages:
            apartment_url = "https://www.puntocasa.net/" + apartment_page['url']
            yield Request(url=apartment_url, callback=self.populate_item,
                          meta={'landlord_phone': apartment_page["landlord_phone"]})

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        external_id = response.xpath('normalize-space(.//div[contains(@class, "text_rif")]//text())')[
            0].extract().replace("Rif. ", "")
        address = response.xpath('.//h3/text()').extract()[0]
        title = response.xpath('.//h1/text()').extract()
        intro_description = response.xpath('normalize-space(string(.//div[contains(@class, "intro")]))').extract()
        intro_description_str = str("".join(intro_description))
        second_description = response.xpath('normalize-space(string(.//div[contains(@id, "testoannuncio")]))').extract()
        second_description_str = "".join(second_description).strip()
        description = intro_description_str + second_description_str
        description = description.encode("ascii", "ignore").decode()
        property_type = 'apartment'
        square_meters_str = response.xpath('.//p[contains(@class, "right")]/text()')[1].extract()
        square_meters = int(square_meters_str[1:])
        room_count = int(response.xpath('.//div[contains(@class, "ico_immobile_right")]//div[contains(@class, "spazio_left")]//span[contains(@class, "loc_up")]/text()')[0].extract())
        rent = response.xpath('.//p[contains(@class, "prezzo_lista")]/text()')[0].extract()
        rent = rent.replace("€ ", "")
        rent = rent.replace(".", "")
        rent = rent.replace(",00", "")
        rent = int(rent)
        images_url = response.xpath('.//a[contains(@class, "gallery_item")]/@href').extract()
        images = ["https://www.puntocasa.net/" + image for image in images_url]
        landlord_name = 'Punto Casa'
        landlord_number = response.meta.get("landlord_phone")
        energy_label = response.xpath('.//div[contains(@class, "spazio_right")]//span[contains(@class, "left")]/text()')[0].extract()

        key = []
        value = []
        for items in response.xpath('.//div[contains(@id, "caratteristiche")]//tr'):
            key.append(items.xpath('td[1]/text()').extract_first())
            value.append(items.xpath('td[2]/text()').extract_first())
        final_key = []
        for single_key in key:
            single_key = single_key.replace(":", "")
            final_key.append(single_key.strip())

        correct_address = address.replace(" - ", ",")
        lon, lat = extract_location_from_address(correct_address)
        latitude = str(lat)
        longitude = str(lon)
        zipcode, city, address_final = extract_location_from_coordinates(longitude, latitude)

        table_dictionary = dict(zip(final_key, value))

        floor = table_dictionary['Piano']
        if table_dictionary.get('Box Auto'):
            parking = True
        else:
            parking = False

        if table_dictionary.get('Terrazzo'):
            terrace = True
        else:
            terrace = False

        if table_dictionary.get('Totale_mq_balconi'):
            balcony = True
        else:
            balcony = False
        if table_dictionary.get('Lavastoviglie'):
            dishwasher = True
        else:
            dishwasher = False
        if table_dictionary.get('Lavatrice'):
            washing_machine = True
        else:
            washing_machine = False

        if table_dictionary['Arredato'] is None:
            furnished = True
        elif table_dictionary['Arredato'] == 'Non arredato':
            furnished = False
        else:
            furnished = True

        landlord_email = response.xpath('.//div[contains(@class, "bg_footer2")]//p//a/text()').extract()

        # # MetaData
        item_loader.add_value("external_link", response.url)  # String
        item_loader.add_value("external_source", self.external_source)  # String

        item_loader.add_value("external_id", external_id)  # String
        item_loader.add_value("position", self.position)  # Int
        item_loader.add_value("title", title)  # String
        item_loader.add_value("description", description)  # String

        # # Property Details
        item_loader.add_value("city", city)  # String
        item_loader.add_value("zipcode", zipcode)  # String
        item_loader.add_value("address", address_final)  # String
        item_loader.add_value("latitude", latitude)  # String
        item_loader.add_value("longitude", longitude)  # String
        item_loader.add_value("floor", floor)  # String
        item_loader.add_value("property_type",
                              property_type)  # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters)  # Int
        item_loader.add_value("room_count", room_count)  # Int
        # item_loader.add_value("bathroom_count", bathroom_count)  # Int

        # if pets_allowed:
        # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        if furnished:
            item_loader.add_value("furnished", furnished)  # Boolean
        if parking:
            item_loader.add_value("parking", parking)  # Boolean
        # if elevator:
        # item_loader.add_value("elevator", elevator) # Boolean
        if balcony:
            item_loader.add_value("balcony", balcony)  # Boolean
        if terrace:
            item_loader.add_value("terrace", terrace)  # Boolean
        # if swimming_pool:
        # item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        if washing_machine:
            item_loader.add_value("washing_machine", washing_machine)  # Boolean
        if dishwasher:
            item_loader.add_value("dishwasher", dishwasher)  # Boolean

        # # Images
        item_loader.add_value("images", images)  # Array
        item_loader.add_value("external_images_count", len(images))  # Int
        # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent)  # Int
        # item_loader.add_value("deposit", rent)  # Int
        # item_loader.add_value("prepaid_rent", rent)  # Int
        # item_loader.add_value("utilities", rent)  # Int
        item_loader.add_value("currency", "EUR")  # String

        # item_loader.add_value("water_cost", water_cost) # Int
        # item_loader.add_value("heating_cost", heating_cost) # Int
        item_loader.add_value("energy_label", energy_label)  # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name)  # String
        item_loader.add_value("landlord_phone", landlord_number)  # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
