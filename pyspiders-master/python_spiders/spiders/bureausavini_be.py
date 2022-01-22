# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy 
from scrapy import Request
from datetime import datetime 
from ..loaders import ListingLoader
from ..helper import extract_rent_currency, extract_number_only, format_date
import re
import js2xml
import lxml.etree
from ..user_agents import random_user_agent
from parsel import Selector


class BureausaviniSpider(scrapy.Spider):
    name = 'bureausavini_be'
    allowed_domains = ['www.bureausavini.be']
    start_urls = ['https://www.bureausavini.be/?p=listerBiens&action=L']
    execution_type = 'testing'
    country = 'belgium'
    locale = 'fr'
    position = 0
    thousand_separator = '.' 
    scale_separator = ','

    def start_requests(self):
        start_urls = [
            {
                'url': 'https://www.bureausavini.be/index.php?p=listerBiens&action=L&sector=A',
                'property_type': 'apartment',
            },
            {
                'url': 'https://www.bureausavini.be/index.php?p=listerBiens&action=L&sector=M',
                'property_type': 'house',
            },
            {
                'url': 'https://www.bureausavini.be/index.php?p=listerBiens&action=L&sector=V',
                'property_type': 'house',
            },
        ]
 
        for url in start_urls:
            yield Request(url=url.get('url'), 
                          callback=self.parse,
                          meta={'response_url': url.get('url'),
                                'property_type': url.get('property_type')})

    def parse(self, response, **kwargs):
        listings = response.xpath('.//a[contains(@href, "bien=")]/@href').extract()
        for listing in listings:
            url = response.urljoin(listing)
            yield Request(url=url,
                          callback=self.parse_listing, 
                          meta={'response_url': url,
                                'property_type': response.meta["property_type"]}) 

    def parse_listing(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.meta.get('response_url'))
        item_loader.add_value('external_id', extract_number_only(response.xpath('.//*[contains(text(), "Réf. :")]/text()').extract_first()))
        item_loader.add_value('property_type', response.meta["property_type"])
        item_loader.add_xpath('title', './/*[@class="resutls-table-title"]/text()')
        
        description = "".join(response.xpath('//div[@class="text-descr"]//text()').getall())
        if description:
            item_loader.add_value('description', description.strip())
 
        # item_loader.add_xpath('address', './/div[@id="mapid"]/../../div/b/text()')
        # address = response.xpath('.//div[@id="mapid"]/../../div/b/text()').extract_first()
        # if address:
        #     zipcode = address.split(" - ")[-1].strip().split(" ")[0]
        #     city = address.split(zipcode)[1].strip()
        #     item_loader.add_value('zipcode', zipcode)
        #     item_loader.add_value('city', city)
            
        item_loader.add_xpath('rent_string', './/*[contains(text(), "Loyer")]/text()')
        item_loader.add_xpath('images', './/div[@class="fotorama"]//a/@href')

        address=response.xpath("//div[@style='text-transform: capitalize;']/b/text()").get()
        if address: 
           city=address.split()[-1]
           zipcode=address.split("-")[1].split()[0].strip()
           item_loader.add_value("address",address)
           item_loader.add_value("city",city) 
           item_loader.add_value("zipcode",zipcode)
        



        utilities = response.xpath("//font//text()[contains(.,'Charges')]").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split(":")[1].split("€")[0].strip())
        else:
            utilities = re.findall(r'\d+(?= euro de charges communes copropriété.)',item_loader.get_output_value('description'))
            if utilities:
                item_loader.add_value('utilities', utilities[0])

        # financial_and_legal = response.xpath('.//*[contains(text(), "Loyer")]/following-sibling::text()').extract()
        financial_and_legal = response.xpath('.//*[contains(text(), "Aspects Financiers et légaux")]/../../div[@class="panel-body"]//text()').extract()
        for text_i in financial_and_legal:
            if "date de disponibilité :" in text_i.lower():
                available_date = re.findall(r'\d{2}[\/\-]\d{2}[\/\-]\d{4}',text_i)
                if len(available_date)>0:
                    item_loader.add_value('available_date',format_date(available_date[0], date_format='%d/%m/%Y'))


        exterior_description = response.xpath('.//*[contains(text(), "Description extérieure")]/../../div[@class="panel-body"]//text()').extract()
        for text_i in exterior_description:
            if "nombre de place de parking" in text_i.lower():
                if int(extract_number_only(text_i)) > 0:
                    item_loader.add_value('parking', True)
                else:
                    item_loader.add_value('parking', False)

        interior_description = response.xpath('//b[contains(text(),"Description intérieure")]/../../div[@class="panel-body"]/font[descendant-or-self::text()]/text()').extract()
        for item in interior_description:
            
            if "Surface habitable" in item:
                item_loader.add_value('square_meters', item.split('.')[0])
                
            if "chambre(s)" in item:
                item_loader.add_value('room_count', item.split('chambre(s)')[0])
            elif "Chambre" in item:
                item_loader.add_value("room_count", item.split(":")[0].strip().split(" ")[-1])
            elif "chambre" in description.lower():
                item_loader.add_value("room_count", description.split("chambre")[0].strip().split(" ")[-1])
                
            if "salle(s) de bains" in item:
                item_loader.add_value('bathroom_count', extract_number_only(item.split('salle(s) de bains')[0]))

        energy_label = response.xpath('.//b[contains(text(),"Certificats et attestations")]/../../div[@class="panel-body"]/font[contains(text(),"PEB spécifique")]/text()').extract_first()
        if energy_label:
            item_loader.add_value('energy_label', energy_label.split('PEB total')[0].split(' : ')[1])

        javascript = response.xpath('.//script[contains(text(), "showMap")]/text()').extract_first()
        xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
        xml_selector = Selector(text=xml)
        item_loader.add_value('latitude', xml_selector.xpath('.//identifier[@name="showMap"]/../../arguments/number/@value').extract()[0])
        item_loader.add_value('longitude', xml_selector.xpath('.//identifier[@name="showMap"]/../../arguments/number/@value').extract()[1])


        item_loader.add_value('landlord_name', "Bureausavini")
        item_loader.add_value('landlord_phone', "071535796")
        item_loader.add_value('landlord_email', "info@bureausavini.be")

        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "Bureausavini_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
