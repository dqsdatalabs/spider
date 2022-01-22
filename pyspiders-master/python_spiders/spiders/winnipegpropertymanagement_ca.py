import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader
import requests


class WinnipegpropertymanagementCaSpider(scrapy.Spider):
    name = 'winnipegpropertymanagement_ca'
    allowed_domains = ['winnipegpropertymanagement.ca']
    start_urls = ['https://www.winnipegpropertymanagement.ca/rentals/']
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    def parse(self, response):
        for appartment in response.css("#cs-content > div.x-section.e17-30.mh-0.mh-3.mh-4 > div > div > div > div > div > div"):
            url = appartment.css('div.h-single-prop-title > a').attrib['href']
            yield Request(url, callback=self.populate_item)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        id = response.xpath("//link[@rel='shortlink']/@href").get()

        title = response.css(
            '#x-section-2 > div > div:nth-child(1) > div.u-street-name::text').get()

        rent = response.css(
            '#x-section-2 > div > div:nth-child(2) > div.u-str-rent > span:nth-child(1)::text').get().split("$")[1]

        if "." in rent:
            rent = rent.split(".")[0]

        try:
            space = response.css(
                'div.u-sqft::text').get().strip().split(' ')[0]
            space = int(int(space)/10.7639)
        except:
            space = None

        # # if "." in rent:
        # #     rent = rent.split(".")[0]

        description = ""
        description_array = response.css(
            "#x-section-6 > div > div > div::text").extract()

        if "house" in description_array[0]:
            property_type = 'house'
        else:
            property_type = 'apartment'

        for item in description_array:
            description += item

        BnB = response.css(
            '#x-section-2 > div > div:nth-child(2) > div.u-str-br > span::text').get().strip()
        beds = BnB.split(' Bed')[0][0]
        baths = BnB.split(' Bed ')[1][0]

        try:
            coords = response.css(
                '#x-google-map-1 > div.x-google-map-marker::attr(data-x-params)').get()
            lat = coords.split('lat":"')[1].split('",')[0]
            lng = coords.split('lng":"')[1].split('",')[0]
        except:
            pass

        available_date = response.css(
            '#x-section-2 > div > div:nth-child(1) > div.u-avail > span::text').get()

        pets = None
        if "PET FRIENDLY!" in description:
            pets = True

        washer = None
        if "dishwasher" in description:
            washer = True

        balcony = None
        if "balcony" in description:
            balcony = True

        pool = None
        if "pool" in description:
            pool = True

        parking = None
        if "parking spot" in description:
            parking = True

        terrace = None
        if "terrace" in description:
            terrace = True

        laundry = None
        if "laundry" in description:
            laundry = True

        imgs = response.css(
            'ul.x-slides > li > img::attr(src)').extract()

        responseGeocode = requests.get(
            f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={lng},{lat}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        zipcode = responseGeocodeData['address']['Postal']
        city = responseGeocodeData['address']['City']
        address = responseGeocodeData['address']['Match_addr']

        # MetaData
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", id.split('=')[-1])
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)

        # Property Details
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("room_count", beds)
        item_loader.add_value("bathroom_count", baths)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("square_meters", int(int(space)*10.764))
        item_loader.add_value("available_date", available_date)

        item_loader.add_value("balcony", balcony)
        item_loader.add_value("pets_allowed", pets)
        item_loader.add_value("dishwasher", washer)
        item_loader.add_value("parking", parking)
        item_loader.add_value("swimming_pool", pool)
        item_loader.add_value("terrace", terrace)
        item_loader.add_value("washing_machine", laundry)

        item_loader.add_value("latitude", lat)
        item_loader.add_value("longitude", lng)

        # # # item_loader.add_value("energy_label", energy)

        # Images
        item_loader.add_value("images", imgs)
        item_loader.add_value("external_images_count", len(imgs))

        # Monetary Status
        item_loader.add_value("rent", int(rent))
        # item_loader.add_value("deposit", int(deposit))
        item_loader.add_value("currency", "CAD")

        # LandLord Details
        item_loader.add_value("landlord_phone", "1-844-415-6200")
        item_loader.add_value("landlord_email", "info@upperedgepm.com")
        item_loader.add_value("landlord_name", "Upper Edge")

        yield item_loader.load_item()
