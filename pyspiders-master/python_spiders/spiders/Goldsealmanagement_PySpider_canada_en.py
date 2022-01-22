import re
from io import StringIO

import requests
import scrapy
from scrapy import Spider, Request, FormRequest
from ..loaders import ListingLoader
import json



class Goldsealmanagement_PySpider_canada_en(scrapy.Spider):
    pos = 1
    name = 'Goldsealmanagement'
    allowed_domains = ['goldsealmanagement.com']
    country = 'canada'
    locale = 'en'
    execution_type = 'testing'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"

    def start_requests(self):
        start_urls = ['https://goldsealmanagement.com/search/?Baths=0&PriceMin=&PriceMax=&orderby=priceasc']
        for url in start_urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response, **kwargs):
        global counter
        urls = response.xpath('.//div[@class="ypnh-property-listing"]//a//@href').extract()
        for x in range(len(urls)):
            url = urls[x]
            yield Request(url=url, callback=self.parse_area)

    def parse_area(self, response):
        links = response.xpath('.//div[@class="ypnh-listing-in-building col-lg-4 col-sm-4"]/a/@href').extract()
        for x in range(len(links)):
            yield Request(url=links[x]+"#"+f"{str(x)}", callback=self.rec,dont_filter=True)


    def rec(self,response):
        av = response.xpath('.//div[@class="col-sm-12 col-xs-6"]/p/text()').extract()
        beds = response.xpath('.//h1[@class="ypnh-details-title"]/text()').extract()
        rent = response.xpath('.//span[@itemprop="priceRange"]/text()').extract()[0].replace("Starting From $", "")
        prop = response.xpath('.//span[@itemprop="description"]/text()').extract()
        imgs = response.xpath('.//div[@class="carousel-inner"]/div//a/img/@src').extract()
        for i in av:
            if "Available" in i and len(rent):
                item_loader = ListingLoader(response=response)
                item_loader.add_value('external_link', response.url)
                item_loader.add_value('external_source', self.external_source)
                item_loader.add_value("landlord_name", "Goldseal Management")
                phone = response.xpath('.//span[@class="details-phone"]/text()').extract()[0]
                item_loader.add_value("landlord_email", "brookwell@gsmi.ca")
                item_loader.add_value("landlord_phone", phone)
                title = "".join(response.xpath('.//h1[@class="ypnh-details-title"]/text()').extract()).strip()
                if "Apartment" in title:
                    item_loader.add_value("property_type", "apartment")
                if "Townhouse" in title:
                    item_loader.add_value("property_type", "house")
                item_loader.add_value("title", title)
                address = response.xpath('.//span[@class="ypnh-breadcrumbs"]/ol/li/span/text()').extract()[0]
                responseGeocode = requests.get(
                    f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/find?f=json&text={address}&maxLocations=1")
                responseGeocodeData = responseGeocode.json()
                longitude = responseGeocodeData['locations'][0]['feature']['geometry']['x']
                latitude = responseGeocodeData['locations'][0]['feature']['geometry']['y']
                responseGeocode = requests.get(
                    f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
                responseGeocodeData = responseGeocode.json()
                zipcode = responseGeocodeData['address']['Postal'] + responseGeocodeData['address']['PostalExt']
                address = responseGeocodeData['address']['Match_addr']
                city = responseGeocodeData['address']['City']
                longitude = str(longitude)
                latitude = str(latitude)
                item_loader.add_value("address", address)
                item_loader.add_value("zipcode", zipcode)
                item_loader.add_value("city", city)
                item_loader.add_value("longitude", longitude)
                item_loader.add_value("latitude", latitude)
                desc = response.xpath('.//p[@class="property-desc"]/text()').extract()[0]
                desc = desc[0:len(desc) - 322]
                item_loader.add_value("description", desc)
                item_loader.add_value("images", imgs)
                item_loader.add_value("external_images_count", len(imgs))
                item_loader.add_value("rent", int(rent))
                item_loader.add_value("currency", "EUR")
                for j in prop:
                    if 'pets allowed' in j:
                        item_loader.add_value("pets_allowed", True)
                    if "SQFT" in j:
                        met = j.replace("From ", "").replace(" SQFT", "")
                        item_loader.add_value("square_meters", int(int(met) / 10.7639))
                    if "Bathroom" in j:
                        bath = j.replace(" Bathroom", "").replace("Baths: ", "")
                        try:
                            item_loader.add_value("bathroom_count", int(bath))
                        except:
                            item_loader.add_value("bathroom_count", 2)
                # beds = response.xpath('.//h1[@class="ypnh-details-title"]/text()').extract()
                for y in beds:
                    if "Bedroom" in y:
                        bed = re.findall(r'\d+', y)[0]
                        item_loader.add_value("room_count", int(bed))
                amen = response.xpath('.//div[@class="col-sm-4"]/ul/li/text()').extract()
                for i in amen:
                    if "parking" in i:
                        item_loader.add_value("parking", True)
                    if "Balcony" in i:
                        item_loader.add_value("balcony", True)
                    if "washer" in i:
                        item_loader.add_value("washing_machine", True)
                    if "Elevator" in i:
                        item_loader.add_value("elevator", True)
                floor = response.xpath('.//div[@class="col-sm-3"]/a/@href').extract()
                item_loader.add_value("floor_plan_images", floor)
                item_loader.add_value("position", self.pos)
                self.pos += 1
                yield item_loader.load_item()

