# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re
import scrapy
from ..helper import extract_number_only, extract_rent_currency
import lxml.etree
import js2xml
from math import ceil
from scrapy import Selector
from ..loaders import ListingLoader


class NeogestimmoFrSpider(scrapy.Spider):
    name = "neogestimmo_fr"
    allowed_domains = ['neogestimmo.fr']
    start_urls = ['https://neogestimmo.fr/']
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    thousand_separator = '.'
    scale_separator = ','
    position = 0
    external_source = 'Neogestimmo_PySpider_france_fr'

    def start_requests(self):

        start_url = ["https://www.neogestimmo.fr/location/1/0"]
        for url in start_url:
            yield scrapy.Request(url=url,
                                 callback=self.parse)

    def parse(self, response, **kwargs):
        listings = response.xpath("//div[@class='row property']")
        for property_item in listings:
            property_url = property_item.xpath(".//div[@class='col-md-7']/a/@href").extract_first()
            rent_original = property_item.xpath(".//span[contains(@title,'Charges Comprises')]/../text()").extract_first()
            if rent_original:
                rent_original = rent_original.replace(" ", "")
            # Not scraping the 'parking' and 'box' types
            if "parking" not in property_url and "box" not in property_url:
                yield scrapy.Request(
                    url = property_url,
                    callback=self.get_property_details,
                    meta={'property_url': property_url,
                          'rent_original': rent_original})

        next_page = response.xpath("//a[contains(text(),'Suivante >')]/@href").extract_first()  
        if next_page:
            next_page_url = "https://www.neogestimmo.fr" + next_page
            yield response.follow(
                url=next_page_url,
                callback=self.parse)

    def get_property_details(self, response):

        item_loader = ListingLoader(response=response)
        status = "".join(response.xpath("//h1[contains(.,'Boutique ') or contains(.,'Bureau')]//text()").getall())
        if status:
            return 
        property_url = response.meta.get('property_url')
        external_id = re.findall(r'/(nova\d+)-', property_url)
        if external_id:
            item_loader.add_value('external_id', external_id)

        item_loader.add_value('external_link', property_url)
        title = response.xpath("//title/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        images = response.xpath(".//a[@class='fancybox']/img/@src").extract()
        images = [response.urljoin(image) for image in images]
        item_loader.add_value('images', images)


        zipcode = response.xpath("//h1//span[contains(@title,'Charges Comprises')]//following-sibling::text()").get()
        if zipcode:
            zipcode="".join(zipcode.split(" ")[-2:-1])
            item_loader.add_value('zipcode', zipcode)

        city = response.xpath("//h1//span[contains(@title,'Charges Comprises')]//following-sibling::text()").get()
        if city:
            city="".join(city.split(" ")[-1:])
            item_loader.add_value('city', city)
            address = city
            if zipcode:
                address += " - "+zipcode
            item_loader.add_value('address', address)

        # area, zipcode = re.findall(r'-\d{1}-\w+-(.+)-(\d{5})', property_url)[0]
        # try:
        #     if area and zipcode:
        #         item_loader.add_value('address', area + ", " + zipcode)
        #         item_loader.add_value('city', area)
        #         item_loader.add_value('zipcode', zipcode)
        # except:
        #     pass
        
        description = ".".join(response.xpath(".//meta[@name='description']/@content").extract_first().split(".")[:-2])
        if description:
            item_loader.add_value('description', description)
        else:
            description = "".join(response.xpath("//div[@class='col-md-12 col-xs-12']/text()").getall())
            if description:
                item_loader.add_value('description', description.strip().replace('\n', ''))
        if "commerçante" in description.lower():
            return

        javascript = response.xpath('.//*[contains(text(),"carte")][2]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            selector = Selector(text=xml)
            latitude = selector.xpath('.//array/number[1]/@value').get()
            longitude = selector.xpath('.//array/number[2]/@value').get()
            item_loader.add_value('latitude', latitude)
            item_loader.add_value('longitude', longitude)
        
        floor = response.xpath(".//img[contains(@src,'immeuble')]/../text()").extract_first()
        if floor:
            item_loader.add_value('floor', floor.split()[0])
        
        square_meters = response.xpath(".//img[contains(@src,'metre')]/../text()").extract_first()
        if square_meters:
            square_meters = str(int(ceil(float(extract_number_only(square_meters,thousand_separator=',',scale_separator='.')))))
            item_loader.add_value('square_meters', square_meters)
        
        studio_check = response.xpath(".//img[contains(@src,'plan')]/../text()").extract_first()
        if studio_check == " studio":
            item_loader.add_value('property_type', 'studio')
            item_loader.add_value('room_count', "1")
        
        bedrooms = response.xpath(".//img[contains(@src,'lit')]/../text()").extract_first()
        if bedrooms:
            item_loader.add_value('room_count', bedrooms[0])
        
        bathrooms = response.xpath(".//img[contains(@src,'douche')]/../text()").extract_first()
        if bathrooms:
            item_loader.add_value('bathroom_count', bathrooms)

        # https://www.neogestimmo.fr/nova388-location-appartement-4-pieces-clamart-92140/appartement-390.html
        elevator = response.xpath(".//img[contains(@src,'ascenseur')]").extract()
        if elevator:
            item_loader.add_value('elevator', True)

        charges = response.xpath('.//h4[contains(text()," CHARGES / TAXES / DÉPENSES ")]/../following-sibling::div/div[@class="panel-body"]/text()').extract()
        for charge in charges:
            if 'Dépôt de garantie' in charge:
                item_loader.add_value('deposit', extract_number_only(charge,thousand_separator=' ',scale_separator=',')[:-2])

        item_loader.add_value('landlord_name', 'Neogestimmo')
        item_loader.add_value('landlord_phone', '0148064123')
        
        apartment_types = ["appartement", "apartment", "flat",
                           "penthouse", "duplex", "triplex", 'maisonette']
        house_types = ['chalet', 'bungalow', 'maison', 'house', 'home', ' villa ', 'holiday complex', 'cottage', 'semi-detached', ' detached ' ]
        
        rent = response.xpath("//div[@class='panel-body']//text()[contains(.,'Loyer')]").get()
        if rent:
            rent = rent.split(":")[1].split("€")[0].strip().split(",")[0].strip().replace(" ","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        utilities = response.xpath("//div[@class='panel-body']//text()[contains(.,'Charges')]").get()
        if utilities:
            utilities = utilities.split(":")[1].split("€")[0]
            item_loader.add_value("utilities", utilities)
        
        if any(i in property_url.lower() for i in apartment_types):
            item_loader.add_value('property_type', 'apartment')
        elif any(i in property_url.lower() for i in house_types):
            item_loader.add_value('property_type', 'house')
        
        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "Neogestimmo_PySpider_{}_{}".format(self.country, self.locale))
        return item_loader.load_item()
