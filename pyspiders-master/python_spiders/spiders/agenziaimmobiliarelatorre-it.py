# Author Abdulrahman Abbas


import scrapy

from ..loaders import ListingLoader
from ..helper import extract_number_only, extract_location_from_address, extract_location_from_coordinates


class AgenziaImmobiliareLaTorre(scrapy.Spider):
    name = 'agenziaimmobiliarelatorre_it'
    allowed_domains = ['agenziaimmobiliarelatorre.it']
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = 'agenziaimmobiliarelatorre_PySpider_italy'
    start_urls = [
        'https://www.agenziaimmobiliarelatorre.it/it/immobili?contratto=2&provincia=&tipologia=1&prezzo_min=&prezzo_max=&rif=',
        'https://www.agenziaimmobiliarelatorre.it/it/immobili?contratto=2&provincia=&tipologia=39&prezzo_min=&prezzo_max=&rif=',
    ]
    position = 1

    def parse(self, response):
        apartment_page_links = response.xpath('//div[@class="property-main-box"]/div[@class="property-images-box"]/a')
        yield from response.follow_all(apartment_page_links, self.parse_info)

    def parse_info(self, response):

        item_loader = ListingLoader(response=response)

        description = response.xpath('//div[@class="single-property-details"]/p//text()').get()
        details = response.xpath('//div[@class="amenities-checkbox"]/label//text()').getall()
        property_type = response.xpath('//div[@class="property-header"]/h5/text()').get()

        if "rif" in description:
            address = description.split("rif")
        else:
            address = description.split("RIF")

        final_address = address[0].replace("--", " ")
        longitude, latitude = extract_location_from_address(final_address)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        images = response.xpath('//div[@id="property-detail1-slider"]//a/img//@src').getall()

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("position", self.position)  # Int

        item_loader.add_xpath('title', '//div[@class="banner-inner"]/h2//text()')
        item_loader.add_value('description', description.replace("-", ","))
        item_loader.add_value('address', final_address)
        item_loader.add_xpath('city', '//div[@class="property-header"]/h3//text()')
        item_loader.add_value("latitude", str(latitude))  # String
        item_loader.add_value("longitude", str(longitude))  # String
        item_loader.add_value("zipcode", zipcode)

        item_loader.add_value('currency', "EUR")

        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))


        floor_plan_identifier = response.xpath('//div[@id="property-detail2-slider"]//div[@class="item active"]//img/@alt').get()
        if floor_plan_identifier:
            item_loader.add_xpath("floor_plan_images", '//div[@class="item active"]//img/@src')
        else:
            item_loader.add_value("floor_plan_images", None)

        all_details = {}
        for i in range(len(details) // 2):
            all_details[details[0]] = details[1]
            del details[0]
            del details[0]

        item_loader.add_value('square_meters', extract_number_only(all_details["Superficie: "]))
        item_loader.add_value('room_count', extract_number_only(all_details["Locali/vani: "]))
        item_loader.add_value('bathroom_count', extract_number_only(all_details["Bagni: "]))

        if "Piano: " in all_details:
            item_loader.add_value('floor', extract_number_only(all_details["Piano: "]))
        else:
            item_loader.add_value("floor", None)

        if "Classe energ.: " in all_details:
            item_loader.add_value('energy_label', all_details["Classe energ.: "])
        else:
            item_loader.add_value("energy_label", None)

        if "Spese mensili: " in all_details:
            item_loader.add_value('utilities', extract_number_only(all_details["Spese mensili: "]))
        else:
            item_loader.add_value("utilities", None)

        if "Prezzo: " in all_details:
            item_loader.add_value('rent', extract_number_only(all_details["Prezzo: "]))
        else:
            item_loader.add_value("rent", None)

        if "Rif.: " in all_details:
            item_loader.add_value("external_id", all_details["Rif.: "])
        else:
            item_loader.add_value("external_id", None)

        if "Arredato: " in all_details:
            if all_details["Arredato: "] == "Arredato":
                item_loader.add_value('furnished', True)
            else:
                item_loader.add_value('furnished', False)
        else:
            item_loader.add_value('furnished', False)

        if "Ascensore: " in all_details:
            if all_details["Ascensore: "] == "Sì":
                item_loader.add_value('elevator', True)
            else:
                item_loader.add_value('elevator', False)
        else:
            item_loader.add_value('elevator', False)

        if "Balconi: " in all_details:
            if all_details["Balconi: "] == "Sì":
                item_loader.add_value('balcony', True)
            else:
                item_loader.add_value('balcony', False)
        else:
            item_loader.add_value('balcony', False)

        if "lavatrice" in description or "Lavatrice" in description:
            item_loader.add_value('washing_machine', True)
        else:
            item_loader.add_value("washing_machine", False)

        if property_type == "Appartamento":
            item_loader.add_value('property_type', 'apartment')
        else:
            item_loader.add_value('property_type', 'house')

        item_loader.add_value('landlord_name', "Agenzia Immobiliare La Torre Sas")
        item_loader.add_value('landlord_phone', "3349383132")

        self.position += 1
        yield item_loader.load_item()




