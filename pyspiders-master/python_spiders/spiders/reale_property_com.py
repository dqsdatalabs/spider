import requests
import scrapy
from scrapy import Request

from ..helper import extract_number_only, remove_white_spaces
from ..loaders import ListingLoader


class Reale_propertyComSpider(scrapy.Spider):
    name = 'reale_property_com'
    allowed_domains = ['reale-property.com']
    start_urls = ['https://reale-property.com/immobili/roma/',
                  'https://reale-property.com/immobili/terracina/',
                  'https://reale-property.com/fondi/'
                  ]
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    all_rentals = []

    def parse(self, response):

        for start_url in self.start_urls:
            yield Request(url=start_url,
                          callback=self.parse_area)

    def parse_area(self, response):
        rentals = response.css('.rh_prop_card__details_elementor')
        pages_link = response.css("div.pagination.rhea-pagination-clean > a::attr(href)").extract()
        current_page = response.css("a.real-btn.current::text").extract_first()
        if current_page:
            for rental in rentals:
                status = (rental.css('.rh_prop_card__status::text').extract_first())
                status = status.replace(" ", "")
                status = status.replace('\n', "")
                if 'InVendita' not in (status.replace(" ", "")):
                    url = rental.css('a::attr(href)').extract_first()
                    self.all_rentals.append(url)
            if int(current_page) == len(pages_link):
                for rental in self.all_rentals:
                    yield Request(url=rental,
                                  callback=self.parse_area_pages)
            else:
                next_page = 'https://reale-property.com/immobili/roma/' + "page/" +str(int(current_page)+1)+'/'
                yield Request(url=next_page,
                              callback=self.parse_area)

    def parse_area_pages(self, response):
        item_loader = ListingLoader(response=response)

        rent = response.css('.price::text').extract_first()
        if '\u20ac' in rent:
            rent = rent.split("\u20ac")
            rent = rent[-1].split(' \t\t')
            external_id = ((response.css('.id::text').extract_first()).split("\u00a0"))[1]
            title = ''.join(response.css('.rh_page__title').extract_first().split('\u2013'))
            description = ((
                (" ".join(response.css('.rh_content p::text').extract())).replace("For Info 3932445602", "").replace(
                    "Per Info 3932445602", "").replace("\u2019",'').replace("\u20ac",'')))
            property_type = "apartment"

            square_meters = response.css(".prop_area > div > span.figure::text").extract_first()
            if square_meters:
                square_meters = (square_meters).replace("\n", '')
                square_meters = square_meters.replace("\t", '')
            elif square_meters is None:
                square_meters =1

            room_count = response.css(".prop_bedrooms > div > span.figure::text").extract_first()
            if room_count is None:
                room_count = 1
            bathroom_count = response.css(".prop_bathrooms > div > span.figure::text").extract_first()

            address = remove_white_spaces(response.css(".rh_page__property_address::text").extract_first())
            city = ((response.css('.property-breadcrumbs > ul > li > a::text').extract()))[-1]
            if address is None or address == '':
                address = city
            zipcode = ''
            longitude = ''
            latitude = ''
            try:
                responseGeocode = requests.get(
                    f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find?f=json&text={address}&maxLocations=1")
                responseGeocodeData = responseGeocode.json()

                longitude = responseGeocodeData['locations'][0]['feature']['geometry']['x']
                latitude = responseGeocodeData['locations'][0]['feature']['geometry']['y']

                responseGeocode = requests.get(
                    f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
                responseGeocodeData = responseGeocode.json()
                zipcode = responseGeocodeData['address']['Postal']
                city = responseGeocodeData['address']['City']
                address = responseGeocodeData['address']['Match_addr']

                longitude = str(longitude)
                latitude = str(latitude)
            except:
                pass

            description_arr = (description.split(" "))
            count = 0
            floor = ''
            for i in description_arr:
                if 'floor' in i:
                    if (description_arr[count-1][0]).isnumeric():
                        description_arr[count-1]
                        floor = description_arr[count-1], i
                else:
                    count += 1

            if 'arredata' or 'arredato' or 'ammobiliato' or 'arredato' in description:
                furnished = True
            else:
                furnished = False

            if 'parcheggio' in description:
                parking = True
            else:
                parking = False

            if 'balcon' in description:
                balcony = True
            else:
                balcony = False

            if 'terrazza' in description:
                terrace = True
            else:
                terrace = False

            if 'piscina' in description:
                swimming_pool = True
            else:
                swimming_pool = False

            images = response.css(".slides > li > a::attr(href)").extract()
            external_images_count = len(images)
            currency = "EUR"
            landlord_name = "reale property"
            landlord_phone = '0668809129'

            item_loader.add_value('external_id', external_id)
            item_loader.add_value('external_source', self.external_source)
            item_loader.add_value('external_link', response.url)
            item_loader.add_value('title', title)
            item_loader.add_value('description', description)

            item_loader.add_value('property_type', property_type)
            item_loader.add_value('square_meters', int(square_meters))
            item_loader.add_value('room_count', room_count)
            item_loader.add_value('bathroom_count', bathroom_count)

            item_loader.add_value('address', address)
            item_loader.add_value('city', city)
            item_loader.add_value('zipcode', zipcode)

            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)

            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", external_images_count)

            # Monetary Status
            item_loader.add_value("rent",
                                  int("".join((rent)[0].split(","))))
            item_loader.add_value("currency", currency)

            item_loader.add_value("furnished", furnished)
            item_loader.add_value("floor", floor)
            item_loader.add_value("parking", parking)
            item_loader.add_value("balcony", balcony)
            item_loader.add_value("terrace", terrace)
            item_loader.add_value("swimming_pool", swimming_pool)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_phone", '06 68 80 91 29')
            item_loader.add_value("landlord_email", 'realepropertyimmobiliare@gmail.com')

            yield item_loader.load_item()
