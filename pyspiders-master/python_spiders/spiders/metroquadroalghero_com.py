import requests
import scrapy
from scrapy import Request

from ..helper import remove_unicode_char, remove_white_spaces, extract_number_only
from ..loaders import ListingLoader


class MetroquadroalgheroComSpider(scrapy.Spider):
    name = 'metroquadroalghero_com'
    allowed_domains = ['metroquadroalghero.com']
    start_urls = ['https://metroquadroalghero.com/ricerca-immobile/?status=in-affitto&type%5B%5D=residenziale',
                  'https://metroquadroalghero.com/ricerca-immobile/?status=in-affitto-estivo&type%5B%5D=residenziale']
    country = 'italy'
    locale = 'in'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    all_rental = []

    def parse(self, response):

        for start_url in self.start_urls:
            yield Request(url=start_url,
                          callback=self.parse_area)

    def parse_area(self, response):
        pages = response.css('.rh_pagination_classic .real-btn::text').extract()
        current_page = response.css('.current::text').extract_first()
        if current_page:
            if len(pages) != int(current_page):
                rentals = response.css('.clearfix > figure > a::attr(href)').extract()
                for rental in rentals:
                    self.all_rental.append(rental)
                next_url = 'https://metroquadroalghero.com/ricerca-immobile/page/' + str(
                    (int(current_page) + 1)) + '/?status=in-affitto&type%5B0%5D=residenziale'
                yield Request(url=next_url,
                              callback=self.parse_area)
            else:
                rentals = response.css('.clearfix > figure > a::attr(href)').extract()
                for rental in rentals:
                    self.all_rental.append(rental)
                for rental in self.all_rental:
                    yield Request(url=rental,
                                  callback=self.parse_area_pages)

    def parse_area_pages(self, response):
        item_loader = ListingLoader(response=response)
        description = response.css('p+ p strong span::text,.s1 span::text').extract()
        description = " ".join(description)
        if "ttualmente" not in description:
            # automated
            external_link = response.url
            external_id = (response.css('.property-meta-id::text').extract_first()).replace("\n", "")

            # automated
            external_source = self.external_source
            title = remove_unicode_char(response.css('.page-title span::text').extract_first())
            description = " ".join(response.css('.content span::text').extract())
            address = response.css('.wrap .title::text').extract_first()
            responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find?f=json&text={address}&maxLocations=1")
            responseGeocodeData = responseGeocode.json()
            longitude = responseGeocodeData['locations'][0]['feature']['geometry']['x']
            latitude = responseGeocodeData['locations'][0]['feature']['geometry']['y']
            responseGeocode = requests.get(
                f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
            responseGeocodeData = responseGeocode.json()
            zipcode = responseGeocodeData['address']['Postal']
            city = responseGeocodeData['address']['City']
            address = responseGeocodeData['address']['Match_addr']
            latitude  = str(latitude)
            longitude  = str(longitude)
            property_type = "apartment"
            square_meters = remove_unicode_char(response.css('.property-meta-size::text').extract_first())
            square_meters = [int(s) for s in square_meters.split() if s.isdigit()]
            room_count = remove_unicode_char(response.css('.property-meta-bedrooms::text').extract_first())[0]
            room_count = [int(s) for s in room_count.split() if s.isdigit()]
            bathroom_count = remove_unicode_char(response.css('.property-meta-bedrooms::text').extract_first())[0]
            bathroom_count = [int(s) for s in bathroom_count.split() if s.isdigit()]
            images = response.css('#property-detail-flexslider img::attr(src)').extract()
            floor_plan_images = response.css('.floor-plan-map > a::attr(href)').extract()
            external_images_count = len(images)
            rent = extract_number_only(remove_white_spaces(remove_unicode_char(' '.join(response.css(".price-and-type::text").extract()))))
            currency = "EUR"
            water_cost = None
            desc_blocks = response.css('.content span::text').extract()
            for i in desc_blocks:
                if 'mensili condominiali' in i:
                    water_cost = extract_number_only(''.join(i.split(",")[-2:-1]))
            energy_label = str(response.css(".energy-class .current::text").extract_first())
            pets_allowed = None
            if 'on si accettano animali domestici' in description:
                pets_allowed = False

            amenities = ' '.join(response.css('.arrow-bullet-list a::text').extract())
            if 'Ascensore' in amenities:
                elevator = True
            else:
                elevator = False

            if 'Balcone' in amenities:
                balcony = True
            else:
                balcony = False

            if 'Arredato' in amenities:
                furnished = True
            else:
                furnished = False
            floor = response.css('.floor-plan-title h3::text').extract_first()
            landlord_name = response.css('.property-agent-title::text').extract_first()
            landlord_email = 'info@metroquadroalghero.com'
            landlord_phone = response.css('.office a::text').extract_first()

            # --------------------------------#
            # item loaders
            item_loader.add_value('external_link', external_link)
            item_loader.add_value('external_id', external_id)
            item_loader.add_value('external_source', external_source)
            item_loader.add_value('title', title)
            item_loader.add_value('description', description)
            item_loader.add_value('city', city)
            item_loader.add_value('zipcode', zipcode)
            item_loader.add_value('address', address)
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
            item_loader.add_value('property_type', property_type)
            item_loader.add_value('square_meters', square_meters)
            item_loader.add_value('room_count', room_count)
            item_loader.add_value('bathroom_count', bathroom_count)
            item_loader.add_value("images", images)
            item_loader.add_value("water_cost", water_cost)
            item_loader.add_value("floor_plan_images", floor_plan_images)
            item_loader.add_value("external_images_count", external_images_count)
            item_loader.add_value("rent", int(rent))
            item_loader.add_value("rent_string", rent)
            item_loader.add_value("currency", currency)
            item_loader.add_value("energy_label", energy_label)
            item_loader.add_value("pets_allowed", pets_allowed)
            item_loader.add_value("floor", floor)
            item_loader.add_value("elevator", elevator)
            item_loader.add_value("balcony", balcony)
            item_loader.add_value("furnished", furnished)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("landlord_phone", landlord_phone)

            yield item_loader.load_item()
