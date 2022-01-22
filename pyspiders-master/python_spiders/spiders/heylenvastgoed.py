# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import re
import dateparser
import scrapy
from ..helper import remove_unicode_char, remove_white_spaces, currency_parser, extract_number_only
from python_spiders.loaders import ListingLoader
import requests
import json

def extract_city_zipcode(_address):
    zip_city = _address.split(", ")[1]
    zipcode, city = zip_city.split(" ")
    return zipcode, city



class HeylenvastgoedSpider(scrapy.Spider):
    """ heylenvastgoed """
    name = "heylenvastgoed"
    allowed_domains = ["heylenvastgoed.be"]
    start_urls = (
        'http://www.heylenvastgoed.be/',
    )
    execution_type = 'testing'
    country = 'belgium'
    locale ='fr'
    current_page = 1
    position = 0

    def start_requests(self):
        start_urls = [
            {'url': 'https://www.heylenvastgoed.be/nl/huren?type=1&price_from=50&price_till=&bedrooms=&order=newest',\
                 'property_type': 'apartment'},
            {'url': 'https://www.heylenvastgoed.be/nl/huren?type=2&price_from=50&price_till=&bedrooms=&order=newest',\
                 'property_type': 'house'},
            {'url': 'https://www.heylenvastgoed.be/nl/huren?type=3&price_from=50&price_till=&bedrooms=&order=newest',\
                 'property_type': 'studio'},
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse,
                meta={'property_type': url.get('property_type'),
                    'url': url.get('url')}
            )
                
    def parse(self, response):
        listings = response.xpath(
            "//a[@class='c-card__link']/@href[not(contains(.,'#'))]")
        for property_item in listings:
            property_url = property_item.extract()
            yield scrapy.Request(url=property_url,
                                 callback=self.get_details,
                                meta={'property_type': response.meta.get('property_type'),
                                       'url': response.meta.get('url')})

        next_page = response.xpath("//a[@aria-label='Next']/@href").get()
        if next_page:
            yield scrapy.Request(
                url=next_page,
                callback=self.parse,
                meta={
                    'property_type': response.meta.get('property_type')
                }
            )
        
    def get_details(self, response):
        item_loader = ListingLoader(response=response)

        self.position += 1
        property_type = response.meta.get('property_type')
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Heylenvastgoed_PySpider_belgium_fr")
        item_loader.add_xpath("title", "//title/text()")
        item_loader.add_value("property_type", property_type)

        rent = response.xpath("//div[contains(@class,'page-head')]/span[contains(@class,'price')]/text()").get()
        if rent:
            rent = rent.strip().replace(".","")
            item_loader.add_value("rent_string", rent)

        item_loader.add_value("external_id", response.url.split("/")[-1].strip())

        square_meters = response.xpath("//div[@class='c-features__item-inner'][contains(.,'oppervlakte')]/span/text()").get()
        if square_meters:
            meters = square_meters.split('m')[0].strip().replace(",",".").strip()
            item_loader.add_value("square_meters", int(float(meters)))

        room_count = "".join(response.xpath("//div[@class='c-features__item-inner'][contains(.,'Slaapkamer')]/span/text()").getall())
        if room_count:
            item_loader.add_value("room_count",room_count.strip())  
        else:     
            room = "".join(response.xpath("normalize-space(//ul[@class='property-features']/li[@class='rooms']/text())").getall())
            if room:
                item_loader.add_value("room_count",room.strip())

        bathroom_count = "".join(response.xpath("//div[@class='c-features__item-inner'][contains(.,'Badkamer')]/span/text()").getall())
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip())  

        address = "".join(response.xpath("//span[contains(@class,'page-head__address')]/a/text()").get())
        if address:
            item_loader.add_value("address",address.strip())
            item_loader.add_value("city", address.split(" - ")[0].strip())
            item_loader.add_value("zipcode", address.split(" - ")[1].strip())

        description = ''.join(response.xpath("//div[@class='c-description']//p//text()").extract())
        if description:
            item_loader.add_value("description",description.strip()) 

        # floor = ''.join(response.xpath("//div[@class='details-content']/dl/div[dt[.='verdieping(en)']]/dd/text()").extract())
        # if floor:
        #     item_loader.add_value("floor",floor.strip()) 

        energy_label = response.xpath("//div[@class='c-features__item-inner'][contains(.,'Energie')]/span/text()").get()
        if energy_label:
            energy_label = energy_label.strip().split(" ")[0]
            item_loader.add_value("energy_label", energy_label)
        lat_lng = response.xpath("//div[contains(@class,'location__overlay')]/a/@href").extract_first()
        if lat_lng:
            item_loader.add_value("latitude", lat_lng.split("=")[1].split(",")[0])
            item_loader.add_value("longitude",lat_lng.split("=")[1].split(",")[1])

        images = response.xpath("//script[contains(.,'lightbox:')]/text()").get()
        if images:
            images = images.split("lightbox:")[1].split("],")[0].strip().strip(",")
            image = remove_white_spaces(images).split("'src': '")
            for i in range(1, len(image)):
                item_loader.add_value("images", image[i].split("'")[0])

        terrace = ''.join(response.xpath("//div[@class='c-features__item-inner'][contains(.,'Terras')]/span/text()").extract())
        if terrace:
            item_loader.add_value("terrace",True)

        parking = ''.join(response.xpath("//div[contains(@class,'c-features_')][contains(.,'Parkeerplaatsen')]/span/text()[.!='0']").extract())
        if parking:
            item_loader.add_value("parking",True)

        elevator = ''.join(response.xpath("//div[contains(@class,'c-features_')][contains(.,'Lift')]/span/text()").extract())
        if elevator:
            if "ja" in elevator:
                item_loader.add_value("elevator",True)
            elif "nee" in elevator:
                item_loader.add_value("elevator",False)

        furnished = ''.join(response.xpath("//div[contains(@class,'c-features_')][contains(.,'gemeubeld') or contains(.,'Gemeubeld')]/span/text()").extract())
        if furnished:
            if "ja" in furnished.lower():
                item_loader.add_value("furnished",True)
            elif "nee" in furnished.lower():
                item_loader.add_value("furnished",False)

        available_date=response.xpath("//div[contains(@class,'c-features_')][contains(.,'beschikbaarheid') or contains(.,'Beschikbaarheid')]/span/text()").get()
        if available_date:
            date_parsed = dateparser.parse(
                available_date, date_formats=["%d-%m-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)
        item_loader.add_xpath("utilities", "//div[contains(@class,'c-features_')][contains(.,'kosten') or contains(.,'Kosten')]/span/text()")
        
        item_loader.add_value("landlord_name", "Heylen Vastgoed Turnhout")
        item_loader.add_value("landlord_phone", "03 260 46 66")
        item_loader.add_value("landlord_email", "info@heylenvastgoed.be")

        if not item_loader.get_collected_values("latitude"):
            script = response.xpath("//script[contains(.,'lat:')]/text()").get()
            if script:
                item_loader.add_value("latitude", script.split("lat:")[-1].split(",")[0].strip())
                item_loader.add_value("longitude", script.split("long:")[-1].split("}")[0].strip())

        yield item_loader.load_item()