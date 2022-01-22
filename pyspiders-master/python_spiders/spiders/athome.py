# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re
from datetime import datetime
import js2xml
import lxml
import scrapy
from geopy.geocoders import Nominatim
from scrapy import Selector
from ..helper import extract_number_only, format_date, remove_unicode_char, extract_rent_currency, remove_white_spaces
from ..loaders import ListingLoader
from ..user_agents import random_user_agent
import dateparser

class AthomeSpider(scrapy.Spider):

    name = 'athome_lu_disabled'
    allowed_domains = ['athome.lu']
    start_urls = ['https://www.athome.lu/']
    execution_type = 'testing'
    country = 'belgium'
    locale = 'fr'
    thousand_separator = ' '
    scale_separator = ','
    position = 0

    def start_requests(self):
        start_urls = [
            {'url': 'https://www.athome.lu/location/maison',
                'property_type': 'house'},
            {'url': 'https://www.athome.lu/location/appartement',
                'property_type': 'apartment'},
        ]
        for url in start_urls:
            yield scrapy.Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'request_url': url.get('url'),
                                       'property_type': url.get('property_type')})

    def parse(self, response, **kwargs):
        listings = response.xpath('.//link[@itemprop="url"]/@href').getall()
        for property_item in listings:
            yield scrapy.Request(
                url=f"https://www.athome.lu{property_item}",
                callback=self.get_property_details,
                meta={'request_url': f"https://www.athome.lu{property_item}",
                      'property_type': response.meta.get('property_type')}
            )
        next_page_url = response.xpath('.//a[@class="nextPage"]/@href').extract_first()
        if next_page_url:
            yield scrapy.Request(
                url=response.urljoin(next_page_url),
                callback=self.parse,
                meta={'request_url': next_page_url,
                      'property_type': response.meta.get('property_type')}
            )

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.meta.get('request_url'))
        item_loader.add_value('property_type', response.meta.get('property_type'))
        item_loader.add_value('external_id', response.meta.get('request_url').split("id-")[1].split(".")[0])
        agency_remove = response.xpath("//div[@class='collapsed']/p//text()[contains(.,'waucomont.be')]").extract_first()
        if agency_remove:
            return
        javascript = response.xpath('.//script[contains(text(),"lon")]/text()').extract_first()
        city2 = ""
        if javascript:
            try:
                xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
                xml_selector = Selector(text=xml)
                latitude = xml_selector.xpath('.//property[@name="lat"]/string/text()').extract_first()
                longitude = xml_selector.xpath('.//property[@name="lon"]/string/text()').extract_first()
                city = xml_selector.xpath('.//property[@name="cityName"]/string/text()').extract_first()
                if latitude:
                    city2 = city
                    item_loader.add_value('latitude', latitude)
                    item_loader.add_value('longitude', longitude)
                elif not latitude:
                    javascript = response.xpath('(.//script[contains(text(),"lon")]/text())[3]').extract_first()
                    if javascript:
                        try:
                            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
                            xml_selector = Selector(text=xml)
                            latitude = xml_selector.xpath('.//property[@name="lat"]/string/text()').extract_first()
                            longitude = xml_selector.xpath('.//property[@name="lon"]/string/text()').extract_first()
                            city = xml_selector.xpath('.//property[@name="cityName"]/string/text()').extract_first()
                            if latitude:
                                city2 = city
                                item_loader.add_value('latitude', latitude)
                                item_loader.add_value('longitude', longitude)
                        
                        except : pass            
            except : pass
        if city2:
            item_loader.add_value('city', city2)
        else:
            city2 = response.xpath("//h1/text()").get()
            if city2:
                city2 = city2.strip().split(" ")[-1]
                item_loader.add_value("city", city2)
        
        if not item_loader.get_collected_values("city"):
            city = "".join(response.xpath("//h1/text()").getall())
            if city: item_loader.add_value("city", city.split("(")[0].strip().split(" ")[-1].strip())
        
        if not item_loader.get_collected_values("latitude"):
            latitude = response.xpath("//script[contains(.,'\"lat\"')]/text()").get()
            if latitude: 
                item_loader.add_value("latitude", latitude.split('"lat":"')[1].split('"')[0].strip())
                item_loader.add_value("longitude", latitude.split('"lon":"')[1].split('"')[0].strip())
        
        item_loader.add_xpath('address', '(.//div[contains(@class, "AgencyAdress")])[1]/p//text()')

        item_loader.add_xpath('title', './/h1[contains(@class,"KeyInfoBlockStyle__PdpTitle-sc-1o1h56e-2")]/text()')

        rent = response.xpath("//div[contains(text(), 'Loyer')]/../div/div/text()").get()
        if rent:
            rent = rent.replace("€","").strip().replace("\u00a0","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        item_loader.add_xpath('description', './/div[@class="collapsed"]/p//text()')
        item_loader.add_xpath('square_meters', './/div[contains(text(),"Surface habitable")]/..//*[contains(text(), "m²")]/text()')
        item_loader.add_xpath('images', './/div[@class="square"]/a/@href')

        item_loader.add_xpath('bathroom_count', './/div[contains(text(),"Salles de bain")]/../div/div/text()')
        item_loader.add_xpath('bathroom_count', './/i[@class="icon-bath"]/..//span/text()')

        item_loader.add_xpath('floor', './/div[contains(text(), "Etage du bien")]/../div/div/text()')
        item_loader.add_xpath('room_count', './/div[contains(text(),"Nombre de chambre")]/following::div/text()')
        
        # email not alvailable
        item_loader.add_xpath('energy_label', './/div[contains(text(),"Classe énergétique")]/..//span[contains(@class,"energy lu")]/text()')
        
        utilities = response.xpath("//div[contains(@class,'feature-bloc-content')][contains(.,'Charges')]//following-sibling::div//text()[not(contains(.,'NC'))]").extract_first()
        if utilities:
            item_loader.add_value('utilities', utilities)

        # ex https://www.athome.lu/location/maison/dudelange/id-6941344.html
        # parking = response.xpath('.//div[contains(text(),"Place(s) de parking en extérieur") or contains(text(),"Place(s) de parking dans un garage")]/following::div/text()').extract_first()
        parking = response.xpath('.//div[contains(text(),"Place(s) de parking")]/../div/div/text()').extract_first()
        if parking:
            if parking.lower() not in ["oui", "non"] and type(extract_number_only(parking)) == str:
                if int(parking) > 0:
                    item_loader.add_value('parking', True)
            elif parking == 'Oui':
                item_loader.add_value('parking', True)
            elif parking == 'Non':
                item_loader.add_value('parking', False)

        # ex https://www.athome.lu/location/maison/strassen/id-6952452.html
        balcony = response.xpath('.//div[contains(text(), "Balcon")]/../div/div/text()').extract_first()
        if balcony:
            if balcony.lower() == "oui":
                item_loader.add_value('balcony', True)
            elif balcony.lower() == "non":
                item_loader.add_value('balcony', False)

        # ex https://www.athome.lu/location/maison/dudelange/id-6941344.html
        terrace = response.xpath('.//div[contains(text(),"Terrasse")]/following::div/text()').extract_first()
        if terrace and terrace != '0':
            item_loader.add_value('terrace', True)
        
        # https://www.athome.lu/location/maison/dudelange/id-6941344.html
        furnished = response.xpath('.//div[contains(text(),"Meublé")]/following::div/text()').extract_first()
        if furnished:
            if furnished == 'Oui':
                item_loader.add_value('furnished', True)
            elif furnished == 'Non':
                item_loader.add_value('furnished', False)

        # https://www.athome.lu/location/maison/luxembourg/id-6993905.html
        script = response.xpath("//script[contains(.,'availability')]//text()").get()
        available_date = response.xpath("//li/div[contains(.,'Disponibilité')]/following-sibling::div//text()").get()
        date2 = ""
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
        elif script:
            try:
                date2 = script.split('availability":{"')[1].split('"')[2].replace("\\u002F","-")
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
            except: pass
        
        if date2:
            item_loader.add_value("available_date", date2)
        else:
            available_date = response.xpath('.//div[contains(text(),"Disponibilité")]/following::div/text()').extract_first()
            if available_date:
                if re.match(r"^([1-9] |1[0-9]| 2[0-9]|3[0-1])(.|-|/)([1-9] |1[0-2])(.|-|/)20[0-9][0-9]$", available_date):
                    item_loader.add_value('available_date', format_date(available_date))

        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "Athome_PySpider_{}_{}".format(self.country, self.locale))

        if not item_loader.get_collected_values("address"):
            address = response.xpath("//h1/text()[1]").get()
            last = response.xpath("//h1/text()[3]").get()
            if address and last: item_loader.add_value("address", address.strip().split(" ")[-1] + " " + last.strip())
        
        # landlord_name = response.xpath('.//h3[contains(@class,"FormAgencyInfoStyle__AgencyName-sc-10hhmvq-5")]/text()').extract_first()
        # if landlord_name:
        #     item_loader.add_value('landlord_name', landlord_name)
        landlord_name = response.xpath(".//div[contains(@class,'agency-details__name')]/text()").extract_first()
        if landlord_name:
            item_loader.add_value('landlord_name', landlord_name)
        if landlord_name and "waucomont" in landlord_name.lower():
            return
            
        landlord_phone = response.xpath("//script[contains(.,'phone1')]/text()").extract_first()
        if landlord_phone:
            item_loader.add_value('landlord_phone', landlord_phone.split('"phone1":"')[1].split('"')[0])
        landlord_email = response.xpath("//script[contains(.,'email')]/text()").extract_first()
        if landlord_email:
            item_loader.add_value('landlord_email', landlord_email.split('"email":"')[1].split('"')[0])
        
        # yield item_loader.load_item()