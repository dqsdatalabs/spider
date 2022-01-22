# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from ..loaders import ListingLoader
from ..helper import remove_white_spaces, format_date, extract_rent_currency, extract_number_only
import re
import js2xml
import lxml.etree
from parsel import Selector
from ..user_agents import random_user_agent


class IannoncesSpider(scrapy.Spider):
    name = 'iannonces_fr'
    allowed_domains = ['iannonces.fr']
    start_urls = ['https://www.iannonces.fr']
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    thousand_separator = ' '
    scale_separator = ','
    position = 0

    def start_requests(self):
        start_urls = [
            {
                "url": "https://www.iannonces.fr/recherche,basic.htm?idtt=1&annlistepg=1&idqfix=1&idtt=1&idtypebien=1&lang=fr&pres=prestige&px_loyermax=Max&px_loyermin=Min&surf_terrainmax=Max&surf_terrainmin=Min&surfacemax=Max&surfacemin=Min",
                "property_type": "apartment",
            },
            {
                "url": "https://www.iannonces.fr/recherche,basic.htm?idtt=1&annlistepg=1&idqfix=1&idtt=1&idtypebien=2&lang=fr&pres=prestige&px_loyermax=Max&px_loyermin=Min&surf_terrainmax=Max&surf_terrainmin=Min&surfacemax=Max&surfacemin=Min",
                "property_type": "house",
            }
            
        ]
        for url in start_urls:
            yield scrapy.Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'request_url': url.get('url'),
                                       'property_type': url.get('property_type')})

    def parse(self, response, **kwargs):
        for property_url in response.xpath(".//*[contains(@class,'span8')]//a/@href").extract():
            yield scrapy.Request(
                url=property_url,
                callback=self.get_property_details,
                meta={'request_url': property_url,
                      'property_type': response.meta["property_type"]}
            )

        if len(response.xpath(".//*[contains(@class,'span8')]//a")) > 0:
            current_page = re.findall(r"(?<=annlistepg=)\d+", response.meta["request_url"])[0]
            next_page_url = re.sub(r"(?<=annlistepg=)\d+", str(int(current_page) + 1), response.meta["request_url"])
            yield scrapy.Request(
                url=response.urljoin(next_page_url),
                callback=self.parse,
                meta={'request_url': next_page_url,
                      'property_type': response.meta["property_type"]}
            )
            
    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.meta["request_url"])
        item_loader.add_value('property_type', response.meta["property_type"])
        item_loader.add_xpath('description', './/p[@itemprop="description"]/text()')
        item_loader.add_xpath('title', './/title/text()')
        
        rent = "".join(response.xpath('(.//div[contains(@class, "detail-prix")])[1]//text()').getall())
        if rent:
            rent = rent.split("€")[0].strip().replace("\xa0","")
            item_loader.add_value("rent", rent.replace(" ",""))
        item_loader.add_value("currency", "EUR")
        
        available_date = response.xpath('.//*[@class="dt-dispo"]//text()').extract_first()
        if available_date:
            date = format_date(available_date.split(':')[1].strip())
            item_loader.add_value('available_date', date)
        
        external_id = response.xpath('.//*[contains(text(),"Référence")]/text()').extract_first()
        if external_id:
            item_loader.add_value('external_id', external_id.split(':')[1].strip())

        item_loader.add_xpath('square_meters', './/li[@title="Surface"]//div[contains(text(), "m²")]/text()')
        room_count = response.xpath('.//*[contains(text(),"Chambre")]/following-sibling::div/text()').extract_first()
        if room_count:
            item_loader.add_value('room_count', remove_white_spaces(room_count))
        else:
            room_count = response.xpath('.//*[contains(text(),"Pièce")]/following-sibling::div/text()').extract_first()
            if room_count:
                item_loader.add_value('room_count', remove_white_spaces(room_count))
        
        floor = response.xpath('.//*[contains(text(),"Etage")]/following-sibling::div/text()').extract_first()
        if floor:
            item_loader.add_value('floor', remove_white_spaces(floor))
        
        bathroom_count = response.xpath('.//*[contains(text(),"Salle de bain")]/following-sibling::div/text()').extract_first()
        if bathroom_count:
            item_loader.add_value('bathroom_count', remove_white_spaces(bathroom_count))
            
        # https://www.iannonces.fr/annonces/locations/appartement/saint-omer-62/487834.htm
        # bathroom_count
        else:
            bathroom_count = response.xpath('''.//*[contains(text(),"Salle d'eau")]/following-sibling::div/text()''').extract_first()
            if bathroom_count:
                item_loader.add_value('bathroom_count', remove_white_spaces(bathroom_count))
        
        parking = response.xpath('.//*[contains(text(),"Parking")]/following-sibling::div/text()').extract_first()
        if parking and parking.lower == "oui" or parking != '0':
            item_loader.add_value('parking', True)
        else:
            item_loader.add_value('parking', False)

        address = response.xpath("//h1[@itemprop='name']/text()[2]").extract_first()
        if address:
            item_loader.add_value('address', address)
            item_loader.add_value('city', address.split("(")[0].strip())
            item_loader.add_value('zipcode', address.split("(")[1].split(")")[0].strip())
            
        # https://www.iannonces.fr/annonces/locations/maison/aire-sur-la-lys-62/505770.htm
        # terrace
        terrace = response.xpath('.//*[contains(text(),"Terrasse")]/following-sibling::div/text()').extract_first()
        if terrace and terrace.lower()== "oui" or terrace != '0':
            item_loader.add_value('terrace', True)
        else:
            item_loader.add_value('terrace', False)

        item_loader.add_xpath('utilities', './/li[contains(text(),"Charges")]//text()')
        item_loader.add_xpath('deposit', './/*[contains(text(),"Dépôt de garantie")]//text()')
        
        images = response.xpath('.//a[contains(@class,"gallery")]/@href').extract()
        item_loader.add_value('images', list(set(images)))
        item_loader.add_value('external_images_count',len(set(images)))
        
        item_loader.add_value('landlord_phone', '03 21 12 37 37')
        item_loader.add_value('landlord_name', "L'IMMOBILIERE COCQUEMPOT LOCATION GERANCE SYNDIC")
        item_loader.add_value("landlord_email", "contact@icocquempot.fr")
        
        javascript = response.xpath('.//script[contains(text(),"LATITUDE")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            selector = Selector(text=xml)
            latitude = selector.xpath('.//property[@name="LATITUDE_CARTO"]//text()').extract_first()
            longitude = selector.xpath('.//property[@name="LONGITUDE_CARTO"]//text()').extract_first()
            if latitude and longitude:
                item_loader.add_value('latitude', latitude)
                item_loader.add_value('longitude', longitude)

                
        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "Iannonces_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
