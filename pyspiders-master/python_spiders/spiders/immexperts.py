# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
import js2xml
import re
from ..loaders import ListingLoader

def extract_city_zipcode(_address):
    zip_city = _address.split(", ")[1]
    zipcode, city = zip_city.split(" ")
    return zipcode, city

class ImmexpertsSpider(scrapy.Spider):
    name = 'immexperts'
    allowed_domains = ['immexperts']
    execution_type = 'testing'
    country = 'france'
    locale ='fr'
    thousand_separator=','
    scale_separator='.'
    
    def start_requests(self):
        start_urls = [
            {'url': 'https://www.immexperts.be/fr/a-louer?view=list&page=1&ptype=2',
                'property_type': 'apartment'},
            {'url': 'https://www.immexperts.be/fr/a-louer?view=list&page=1&ptype=1',
                'property_type': 'house'},
        ]
        for url in start_urls:
            yield scrapy.Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})
    
    def parse(self, response, **kwargs):
        links = response.xpath('//div[@id="PropertyListRegion"]//div[@class="row-fluid"]//a')
        if links:
            for link in links:  
                url = response.urljoin(link.xpath('./@href').extract_first())
                yield scrapy.Request(
                    url=url,
                    callback=self.get_property_details,
                    meta={'property_type': response.meta.get('property_type')},
                    dont_filter=True
                )
        if response.xpath('//a[contains(text(), ">>")]/@href'):
            next_url = response.urljoin(response.xpath('//a[contains(text(), ">>")]/@href').extract_first())
            yield scrapy.Request(
                url=next_url,
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')},
                dont_filter=True
                )

    def get_property_details(self, response):
        external_link = response.url
        external_id = response.url.split("id=")[1].split("&")[0]
        property_type = response.meta.get('property_type')
        title = ''.join(response.xpath('//div[@id="PropertyRegion"]//div[@class="span8"]//h3/text()').extract())
        title = re.sub(r'[\n\t]+', '', title)
        address_text = ''.join(response.xpath('//div[contains(text(),"Adresse")]/following-sibling::div[@class="value"]/text()').extract())
        if ',' in address_text:
            address = address_text
        else:
            address = "Rue de Troka 480, " + address_text
        zipcode, city = extract_city_zipcode(address)
        detail_texts = ''.join(response.xpath('//div[contains(text(), "Description")]/following-sibling::div/div[@class="field"]//text()').extract())
        images = []
        image_links = response.xpath('//div[@class="galleria"]//img')
        for image_link in image_links:
            image_url = image_link.xpath('./@src').extract_first()
            if image_url not in images:
                images.append(image_url)
        elevator_text = ''.join(response.xpath('//div[contains(text(),"Ascenseur")]/following-sibling::div[@class="value"]/text()').extract()) 
        item_loader = ListingLoader(response=response)
        item_loader.add_value('property_type', property_type)
        item_loader.add_value('title', title)
        item_loader.add_value('external_id', external_id)
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('address', address)
        item_loader.add_xpath('rent_string', '//div[contains(text(),"Prix")]/following-sibling::div[@class="value"]/text()')
        item_loader.add_xpath('description', '//div[contains(text(), "Description")]/following-sibling::div/div[@class="field"]//text()')
        item_loader.add_xpath('square_meters', '//div[contains(text(),"Superficie totale")]/following-sibling::div[@class="value"]/text()')
        item_loader.add_value('images', images)
        if 'terras' in detail_texts.lower():
            item_loader.add_value('terrace', True)
        if 'Oui' in elevator_text:
            item_loader.add_value('elevator', True)
        if 'garage' in detail_texts.lower() or 'park' in detail_texts.lower():
            item_loader.add_value('parking', True)
        room_count = response.xpath("//div[contains(@class,'name')][contains(.,'Chambre')]//following-sibling::div[contains(@class,'value')]//text()").get()
        if room_count:
            room_count = room_count.split(" ")[0]
            item_loader.add_value('room_count', room_count)
        bathroom_count = response.xpath("//div[contains(@class,'name')][contains(.,'salle')]//following-sibling::div[contains(@class,'value')]//text()").get()
        if bathroom_count:
            item_loader.add_value('bathroom_count', bathroom_count)
        utilities = response.xpath("//div[contains(@class,'name')][contains(.,'Charge')]//following-sibling::div[contains(@class,'value')]//text()").get()
        if utilities:
            utilities = utilities.split("€")[0].strip()
            item_loader.add_value("utilities", utilities)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//div[contains(@class,'name')][contains(.,'Disponibilité')]//following-sibling::div[contains(@class,'value')]//text()").getall())
        if available_date:
            if "now" in available_date.lower():
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        latitude_longitude = response.xpath("//script[contains(.,'setView')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('setView([')[1].split(',')[0]
            longitude = latitude_longitude.split('setView([')[1].split(',')[1].split(']')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        item_loader.add_value('landlord_name', 'Immexperts')
        item_loader.add_value('landlord_email', 'bureau@immexperts.be')
        item_loader.add_value('landlord_phone', '+32 (0)85/84 41 91')
        item_loader.add_value('zipcode', zipcode)
        item_loader.add_value('city', city)
        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
        yield item_loader.load_item()



         