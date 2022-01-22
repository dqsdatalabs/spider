# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
import js2xml
import re
from ..loaders import ListingLoader
from python_spiders.helper import remove_unicode_char, extract_rent_currency, format_date

def extract_city_zipcode(_address):
    zip_city = _address.split(", ")[1]
    zipcode, city = zip_city.split(" ")
    return zipcode, city

class ImpactimmaSpider(scrapy.Spider):
    name = 'impactimma'
    allowed_domains = ['impactimma']
    start_urls = ['http://www.impactimma.be/']
    execution_type = 'testing'
    country = 'belgium'
    locale ='nl'
    thousand_separator='.'
    scale_separator=','
    custom_settings = {
        "PROXY_ON":"True",
        "HTTPCACHE_ENABLED": False
    }
    handle_httpstatus_list = [403]
    def start_requests(self):
        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "accept-encoding": "gzip, deflate",
            "accept-language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
            "cache-control": "max-age=0",
            "connection": "keep-alive",
            "cookie": "PHPSESSID=heac6a908f4j7nfjl7ubtpult4; cartId=2650260d425d30f54a8.64320715",
            "host": "www.impactimma.be",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.85 Safari/537.36"
                    }
        
        start_urls = [
            {'url': 'http://www.impactimma.be/?action=list&index.php?ctypmandatmeta=l&action=list&ctypmandatl=1&ctypmandatlm=1&ctypmandatmeta=l&cbien=&ctypmeta=appt',
                'property_type': 'apartment'},
            {'url': 'http://www.impactimma.be/?action=list&index.php?ctypmandatmeta=l&action=list&ctypmandatl=1&ctypmandatlm=1&ctypmandatmeta=l&cbien=&ctypmeta=mai',
                'property_type': 'house'}
        ]
        for url in start_urls:
            yield scrapy.Request(url=url.get('url'),
                                 callback=self.parse,
                                 headers=headers,
                                 meta={'property_type': url.get('property_type')})
    
    def parse(self, response, **kwargs):
        links = response.xpath('//div[@id="portfolio-wrapper"]//div[@class="picture"]/a')
        for link in links:
            sale_type = link.xpath('.//span/text()').extract_first('')
            if 'Loué' in sale_type: 
                continue
            url = response.urljoin(link.xpath('./@href').extract_first())
            title = link.xpath('./@title').extract_first()
            yield scrapy.Request(
                url=url,
                callback=self.get_property_details,
                meta={'property_type': response.meta.get('property_type'), 'title': title},
                dont_filter=True
            )
        if response.xpath('//div[@id="textbox"]//a[contains(text(), "Suivant")]'):
            next_link = response.urljoin(response.xpath('//div[@id="textbox"]//a[contains(text(), "Suivant")]/@href').extract_first())
            yield scrapy.Request(
                url=next_link,
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type'), 'title': title},
                dont_filter=True
            )

    def get_property_details(self, response):
        external_id = response.xpath('//p[contains(text(), "Réf")]/text()').extract_first()
        if external_id:
            external_id = external_id.replace('Réf.:', '')
            property_type = response.meta.get('property_type')
            external_link = response.url
            title = response.meta.get('title') 
            address = "Grand Place 8, 1480 Tubize"
            zipcode, city = extract_city_zipcode(address)
            images = []
            image_links = response.xpath('//div[@id="sliderx"]//ul[@class="slides"]/li/img')
            for image_link in image_links:
                image_url = response.urljoin(image_link.xpath('./@src').extract_first())
                if image_url not in images:
                    images.append(image_url)
            terrace_text = response.xpath('//div[@id="desc"]//li[contains(text(), "Terrasse")]/text()').extract_first('')
            if terrace_text: 
                terrace_v = re.findall(r'\d+', terrace_text, re.S | re.M | re.I)[0]
                if int(terrace_v) > 0:
                    terrace = True
                else:
                    terrace = ''
            else:
                terrace = ''
            item_loader = ListingLoader(response=response)
            item_loader.add_value('external_id', external_id)
            item_loader.add_value('title', title)
            item_loader.add_value('property_type', property_type)
            item_loader.add_value('external_link', external_link)
            item_loader.add_value('address',address)
            item_loader.add_xpath('rent_string', '//div[@id="desc"]//b[contains(text(), "Prix")]/text()')
            item_loader.add_xpath('description', '//div[@id="desc"]/p//text()')
            item_loader.add_xpath('square_meters', '//div[@id="desc"]//li[contains(text(), "Surface habitable")]/text()')
            item_loader.add_value('images', images)
            if terrace:
                item_loader.add_value('terrace', True)
            
            room_count = response.xpath('//ul[@class="check_list"]/li[contains(., "Chambre")]/text()').get()
            if room_count:
                item_loader.add_value('room_count', room_count.split(" ")[0])
                
            bathroom_count = response.xpath("substring-before(//ul[@class='check_list']/li/text()[contains(.,'Salle de douche') or contains(.,'Salle de bain')],' ')").get()
            if bathroom_count:
                item_loader.add_value("bathroom_count", bathroom_count)
            item_loader.add_xpath('utilities', "substring-after(//ul[@class='check_list']/li[contains(.,'Charges:')]/text(),': ')")
            item_loader.add_value('landlord_name', 'Impactimma')
            item_loader.add_value('landlord_email', 'info@impactimma.be')
            item_loader.add_value('landlord_phone', '02/390.05.90')
            item_loader.add_value('zipcode', zipcode)
            item_loader.add_value('city', city)
            item_loader.add_value('external_source', 'Impactimma_PySpider_belgium_nl')
            yield item_loader.load_item()


             