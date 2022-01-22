from typing import Text
import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader
import requests


class DistrictrealtyComSpider(scrapy.Spider):
    name = 'districtrealty_com'
    allowed_domains = ['districtrealty.com']
    start_urls = ['https://www.districtrealty.com/ottawa-apartments/']
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'

    def parse(self, response):
        for appartment in response.css("#main > section.section.sectionResidentialIntro > div > div.blockLocations > div.locationsListing > div.locationsTeasers.isotopeTeasers > div"):
            url = appartment.css("a.propertyTeaser__btnMore").attrib['href']
            yield Request(url,
                          callback=self.populate_item,
                          )

    # 2. SCRAPING level 2
    def populate_item(self, response):

        id = response.xpath("//link[@rel='shortlink']/@href").get()

        title = response.css(
            '#main > section.section.sectionPropertyDetails > div > div.propertyHeader > h1::text').get()

        description = ''
        description_array = response.css(
            "div.sectionBuildingAreaHighlights__text.wysiwyg > p::text").extract()

        for item in description_array:
            if "To book a viewing" in item or "please call" in item or "Register to learn" in item or "Interested in applying" in item:
                pass
            else:
                description += item

        images = response.css(
            'img.propertyGallerySlider__image.propertyGallerySlider__image--landscape::attr(src)').extract()

        lat = response.css('#propertyMap::attr(data-lat)').get()
        lng = response.css('#propertyMap::attr(data-lng)').get()

        responseGeocode = requests.get(
            f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={lng},{lat}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        zipcode = responseGeocodeData['address']['Postal']
        city = responseGeocodeData['address']['City']
        address = responseGeocodeData['address']['Match_addr']

        landlord_name = response.css(
            '#main > section.section.sectionPropertyDetails > div > div.propertyDetails > div.propertyCTA > a.propertyCTA__link.propertyCTA__link--tel > span.propertyCTA__label::text').get().split()[1].split(":")[0]
        landlord_number = response.css(
            '#main > section.section.sectionPropertyDetails > div > div.propertyDetails > div.propertyCTA > a.propertyCTA__link.propertyCTA__link--tel > span.propertyCTA__value::text').get()

        hash = 1
        for item in response.css('table.availableApartments__table'):
            item_loader = ListingLoader(response=response)

            bedrooms = item.css('tr:nth-child(1) > td::text').get()
            bedrooms = str(bedrooms)[0]
            if bedrooms == 'N' or bedrooms == "B" or bedrooms == 0:
                bedrooms = 1

            bathrooms = item.css('tr:nth-child(4) > td::text').get()

            if "NO AVAILABILITY" in str(item.css('tr:nth-child(5) > td::text').get()):
                pass
            else:
                avaialble_date = str(
                    item.css('tr:nth-child(5) > td::text').get())

            rent = str(item.css('tr:nth-child(3) > td::text').get())

            if "$" in rent:
                rent = rent.split("$")[1]

            if "," in rent:
                rent = rent.split(",")
                rent = rent[0]+rent[1]

            try:
                int(rent)
            except:
                continue

            parking = None
            if "parking" in description:
                parking = True

            laundry = None
            if "laundry" in description:
                laundry = True

            # MetaData
            item_loader.add_value("external_link", response.url + f'#{hash}')
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value(
                "external_id", "{}".format(id.split("=")[-1].strip()))
            item_loader.add_value("title", title)
            item_loader.add_value("description", description)

            # Property Details
            item_loader.add_value("property_type", "apartment")
            # item_loader.add_value("square_meters", int(int(int(space))*10.764))
            item_loader.add_value("room_count", bedrooms)
            item_loader.add_value("bathroom_count", str(bathrooms))
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
            item_loader.add_value("available_date", avaialble_date)
            item_loader.add_value("parking", parking)
            # item_loader.add_value("swimming_pool", pool)
            item_loader.add_value("washing_machine", laundry)

            item_loader.add_value("latitude", lat)
            item_loader.add_value("longitude", lng)

            # Images
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

            # Monetary Status
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", "CAD")

            # # LandLord Details
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_phone", landlord_number)
            item_loader.add_value("landlord_email", 'info@districtrealty.com')

            hash += 1

            yield item_loader.load_item()
