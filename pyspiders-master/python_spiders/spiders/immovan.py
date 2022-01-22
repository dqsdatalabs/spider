# -*- coding: utf-8 -*-
# Author: Pankaj Kalania
# Team: Sabertooth

import scrapy, copy, urllib
from ..loaders import ListingLoader
from ..helper import extract_number_only, remove_white_spaces
import re


class ImmovanSpider(scrapy.Spider):
    
    name = 'immovan_be'
    allowed_domains = ['immovan.be']
    start_urls = ['http://www.immovan.be/index.php']
    execution_type = 'testing'
    country = 'belgium'
    locale = 'fr'
    position = 0
    thousand_separator = '.'
    scale_separator = ','
        
    def start_requests(self):
        start_urls = [
            {
                "url": "http://www.immovan.be/index.php?page=0&action=list&ctypmandatmeta=l&ctypmeta=appt&llocalite=&mprixmin=&mprixmax=&cbien=",
                "property_type": "apartment",
            },
            {
                "url": "http://www.immovan.be/index.php?page=0&action=list&ctypmandatmeta=l&ctypmeta=mai&llocalite=&mprixmin=&mprixmax=&cbien=",
                "property_type": "house",
            }
        ]
        for url in start_urls:
            yield scrapy.Request(url=url.get("url"),
                                 callback=self.parse,
                                 meta={'request_url': url.get("url"),
                                       "property_type": url.get("property_type")})
                
    def parse(self, response, **kwargs):
        for property_url in response.xpath('.//a[@class="more-details"]/@href').extract():
            yield scrapy.Request(
                url=property_url,
                callback=self.get_property_details,
                meta={'request_url': property_url,
                      'property_type': response.meta["property_type"]}
            )

        if len(response.xpath('.//a[@class="more-details"]')) > 0:
            current_page = re.findall(r"(?<=page=)\d+", response.meta["request_url"])[0]
            next_page_url = re.sub(r"(?<=page=)\d+", str(int(current_page)+1), response.meta["request_url"])
            yield scrapy.Request(
                url=response.urljoin(next_page_url),
                callback=self.parse,
                meta={'request_url': next_page_url,
                      'property_type': response.meta["property_type"]}
            )

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value('external_link', response.meta.get('request_url'))
        external_id = response.xpath('.//p[contains(text(),"Réf")]/text()').extract_first()
        item_loader.add_value('external_id', external_id.split(':')[-1].strip())
        item_loader.add_value('property_type', response.meta["property_type"])

        item_loader.add_xpath('title', './/*[@id="breadcrumbs"]/../h2/text()')
        item_loader.add_xpath('description', './/div[@id="desc"]//text()')

        city = response.xpath('.//section[@class="titlebar"]//h2/text()').extract_first()
        item_loader.add_value('city', city.split(' - ')[-1])
        item_loader.add_xpath('rent_string', './/li[contains(text(),"Prix:")]/text()')
        item_loader.add_xpath('utilities', './/li[contains(text(),"Charges:")]/text()')
        # item_loader.add_xpath('images', './/div[@class="fotorama"]//img/@src')
        item_loader.add_xpath('images', './/div[@class="slider"]//img/@data-full')

        # dishwasher
        # http://www.immovan.be/fr/annonces-immobilieres/location-non-meublee/mons/appartement/10009/centre-ville--rez-de-chaussee-deux-chambres-et-gar.html
        if response.xpath('.//p[contains(text(), "Lave vaisselle ")]').extract_first():
            item_loader.add_value("dishwasher", True)

        # elevator
        # http://www.immovan.be/fr/annonces-immobilieres/location-non-meublee/mons/studio/10234/10234.html
        if response.xpath('.//p[contains(text(), "Ascenseur")]').extract_first():
            item_loader.add_value("elevator", True)

        # energy_label = response.xpath('.//p[contains(text(),"Total énergie primaire:")]/text()').extract_first()
        energy_label = response.xpath('.//p[contains(text(),"Prestation énergétique:")]/text()').extract_first()
        if energy_label:
            item_loader.add_value('energy_label', energy_label.split(":")[-1])
        room_count = response.xpath('.//li[contains(text(),"Chambre")]/text()').extract_first()
        if room_count:
            item_loader.add_value('room_count', extract_number_only(room_count))
        item_loader.add_xpath('square_meters', './/li[contains(text(),"habitable")]/text()')

        # furnished
        furnish = response.xpath('.//*[(@id="desc")]/h3/text()').extract_first().split(' - ')[0]
        if "non meublée" in furnish:
            item_loader.add_value('furnished', False)
        elif "meublée" in furnish:
            item_loader.add_value('furnished', True)

        # bathroom_count
        bathroom_count = response.xpath('.//li[contains(text(),"Salle de bains")]/text()').extract_first()
        if bathroom_count:
            item_loader.add_value('bathroom_count', extract_number_only(bathroom_count))

        terrace = response.xpath('.//li[contains(text(),"Terrasse")]/text()').extract_first()
        if terrace and "Non" not in terrace:
            item_loader.add_value('terrace', True)

        item_loader.add_value('landlord_name', "Immobilière VANSTHERTEM")
        item_loader.add_value('landlord_email', "info@immovan.be")
        item_loader.add_value('landlord_phone', "065 33 91 27")
        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "Immovan_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
