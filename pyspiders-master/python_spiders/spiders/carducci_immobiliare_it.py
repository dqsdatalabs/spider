# Author Abdulrahman Abbas


import scrapy

from ..loaders import ListingLoader
from ..helper import extract_number_only, extract_lat_long, extract_location_from_coordinates, string_found


class CarducciImmobiliareItSpider(scrapy.Spider):
    name = 'carducci_immobiliare_it'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = 'carducci_immobiliare_PySpider_italy'
    start_urls = ['https://www.carducci-immobiliare.it/?ct_property_type=affitti&ct_state=0&ct_beds=0&ct_price_from&ct_price_to&search-listings=true']

    def parse(self, response):
        page_links = response.xpath('//h5[@class="marB0"]//a')
        yield from response.follow_all(page_links, self.parse_info)



    def parse_info(self, response):
        item_loader = ListingLoader(response=response)
        description = response.xpath('//div[@id="listing-content"]//p//text()').get()
        # address = response.xpath('//p[@class="location marB0"]//text()').get()
        images = response.xpath('//a[@class="gallery-item"]//img/@src').getall()
        square_meters = extract_number_only(response.xpath('//li[@class="row sqft"]//span[@class="right"]/text()').get())
        rent = response.xpath('//h4[@class="price marT0 marB0"]//text()').get()
        position = extract_lat_long(response.xpath('//div[@id="listing-location"]//script//text()').get())
        lat = position[0]
        long = position[1]

        item_loader.add_value('property_type', 'apartment')
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        zipcode, city, address = extract_location_from_coordinates(position[1], position[0])

        item_loader.add_value('longitude', long)
        item_loader.add_value('latitude', lat)
        item_loader.add_value('address', address)
        item_loader.add_value('city', city)
        item_loader.add_value('zipcode', zipcode)
        item_loader.add_xpath('title', '//h2[@class="marT5 marB0"]//text()')
        item_loader.add_value('description', description)

        item_loader.add_value('square_meters', square_meters)
        item_loader.add_xpath('bathroom_count', '//li[@class="row baths"]//span[@class="right"]/text()' )
        item_loader.add_xpath('room_count', '//li[@class="row beds"]//span[@class="right"]/text()')

        if "Ape:" in description:
            x = description.find("Ape:")
            item_loader.add_value('energy_label', description[x+5])
        else:
            item_loader.add_value('energy_label', None)

        furnished = ['arredato']
        item_loader.add_value('furnished', string_found(furnished, description))

        washing_machine = ['Lavanderia']
        item_loader.add_value('washing_machine', string_found(washing_machine, description))

        balcony = ['Balcone']
        item_loader.add_value('balcony', string_found(balcony, description))

        elevator = ['Ascensore']
        item_loader.add_value('elevator', string_found(elevator, description))

        terrace = ['Terrazzo']
        item_loader.add_value('terrace', string_found(terrace, description))

        parking = ['Parcheggio', 'auto', 'Garage']
        item_loader.add_value('parking', string_found(parking, description))

        swimming_pool = ['Piscina']
        item_loader.add_value('swimming_pool', string_found(swimming_pool, description))

        item_loader.add_value('currency', "EUR")
        item_loader.add_value('rent', rent)

        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        item_loader.add_value('landlord_name', "Carducci Immobiliare")
        item_loader.add_value('landlord_phone', "+39 051 6360980")
        item_loader.add_value('landlord_email', "carducci.immobiliare@gmail.com")

        yield item_loader.load_item()




