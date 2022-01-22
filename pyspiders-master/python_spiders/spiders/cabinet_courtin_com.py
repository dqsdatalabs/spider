# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy 
from ..loaders import ListingLoader
import lxml
import js2xml
from scrapy import Selector
import re

class CabinetCourtinComSpider(scrapy.Spider):
    name = "cabinet_courtin_com"
    allowed_domains = ["www.cabinet-courtin.com"]
    start_urls = (
        'https://www.cabinet-courtin.com/immobilier/pays/locations/france.htm',
    )
    execution_type = 'testing'
    country = 'france'
    locale ='fr'
    thousand_separator='.'
    scale_separator=','
    position = 0
    external_source="Cabinet_Courtin_PySpider_france"
 
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url, 
                callback=self.parse)

    def parse(self, response, **kwargs):
        listings = response.xpath('.//div[@itemtype="http://schema.org/Product"]')
        for listing in listings:
            property_url = listing.xpath('.//a/@href').extract_first()
            yield scrapy.Request(
                url=property_url,  
                callback=self.get_property_details, 
                meta={'request_url':property_url}) 

        next_page_url = response.xpath('.//li[@class="active"]/following-sibling::li[@class="refine-page"]/a/@href').extract_first()
        if next_page_url: 
            next_page_url = response.urljoin(next_page_url)
            yield scrapy.Request(url=next_page_url,
                callback=self.parse)

    def get_property_details(self, response, **kwargs):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.meta.get('request_url'))
        item_loader.add_xpath('title','.//p[@itemprop="description"]/text()')
        item_loader.add_xpath("square_meters",'.//div[contains(text(),"Surface")]/following-sibling::div/text()')
        room_count=response.xpath("//div[contains(text(),'Pièce')]/following-sibling::div/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)



        # item_loader.add_xpath("room_count",'.//div[contains(text(),"Chambres")]/following-sibling::div/text()')
        item_loader.add_xpath("bathroom_count",'.//div[contains(text(),"Salle de bain") or contains(text(),"Salles de bain")]/following-sibling::div/text()')
        # item_loader.add_xpath("floor",'.//div[contains(text(),"Etage")]/following-sibling::div/text()')
        item_loader.add_xpath('description','.//p[@itemprop="description"]/text()')
        item_loader.add_xpath('rent_string','.//div[@class="nivo-html-caption"]/text()')

        external_id = response.xpath('.//div[@class="bloc-detail-reference"]/text()').extract_first()
        if external_id:
            item_loader.add_value('external_id',external_id.split(' : ')[-1])

        item_loader.add_xpath('images','.//a[@class="gallery"]/img/@src')

        property_type = response.meta.get('request_url').split('/')[-3]

        if property_type == 'studio':
                item_loader.add_value('property_type','studio')
        elif property_type == 'appartement':
            item_loader.add_value('property_type','apartment')
        elif property_type == 'maison':
            item_loader.add_value('property_type','house')
        else:
            return

        deposit = response.xpath('.//strong[contains(text(),"Dépôt de garantie : ")]/text()').extract_first()
        if deposit:
            item_loader.add_value('deposit',deposit.split(' : ')[-1])

        floor = response.xpath('.//div[contains(text(),"Etage")]/following-sibling::div/text()').extract_first()
        if floor and floor.strip().isdigit():
            item_loader.add_value('floor',floor.strip())

        energy_label = response.xpath('.//div[contains(text(),"kWhEP/m²/an")]/text()').extract_first()
        if energy_label:
            item_loader.add_value('energy_label',energy_label.strip())

        parking = response.xpath('.//div[contains(text(),"Parking")]/following-sibling::div/text()').extract_first()
        if parking:
            if parking in ['non','0']:
                item_loader.add_value('parking',False)
            else:
                item_loader.add_value('parking',True)

        elevator = response.xpath('.//div[contains(text(),"Ascenseur")]/following-sibling::div/text()').extract_first()
        if elevator:
            if elevator in ['non','0']:
                item_loader.add_value('elevator',False)
            else:
                item_loader.add_value('elevator',True)

        city_zip = response.xpath('.//ul//span[@itemprop="title"]/text()').extract()
        if city_zip:            
            city_zip_result = re.findall(r'(?<=Appartement )(.+) \((\d+)\)',city_zip[-1])
            if city_zip_result:
                item_loader.add_value("city", city_zip_result[0][0])
                item_loader.add_value("zipcode", city_zip_result[0][1])
                item_loader.add_value("address", f"{city_zip_result[0][1]} {city_zip_result[0][0]}")
            else:
                city_zip_result = re.findall(r'(?<=Maison / villa )(.+) \((\d+)\)',city_zip[-1])
                if city_zip_result:
                    item_loader.add_value("city", city_zip_result[0][0])
                    item_loader.add_value("zipcode", city_zip_result[0][1])
                    item_loader.add_value("address", f"{city_zip_result[0][1]} {city_zip_result[0][0]}")

        javascript = response.xpath('.//script[contains(text(), "LATITUDE_CARTO")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            xml_selector = Selector(text=xml)
            # print(xml)
            item_loader.add_value('latitude', xml_selector.xpath('.//property[@name="LATITUDE_CARTO"]/string/text()').extract_first().replace(',','.'))
            item_loader.add_value('longitude', xml_selector.xpath('.//property[@name="LONGITUDE_CARTO"]/string/text()').extract_first().replace(',','.'))

        item_loader.add_value("landlord_phone", '03 21 88 18 56')
        item_loader.add_value("landlord_name", 'COURTIN IMMOBILIER')
        item_loader.add_value("landlord_email","referencementprestataire@gmail.com")
		

        item_loader.add_value("external_source", self.external_source)
        self.position += 1
        item_loader.add_value('position', self.position)
        yield item_loader.load_item()
