# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from ..loaders import ListingLoader
from ..helper import extract_number_only
from scrapy import Request, Selector
import lxml 
import js2xml
import re


class AthomepartnersSpider(scrapy.Spider):
    name = 'athomepartners_be'
    allowed_domains = ['www.athomepartners.be']
    start_urls = ['http://www.athomepartners.be/']
    execution_type = 'testing'
    country = 'belgium'
    locale = 'nl'
    thousand_separator = '.'
    scale_separator = ','
    position = 0

    def start_requests(self):
        start_urls = [{
            'url': 'https://www.athomepartners.be/te-huur?part=residentieel&category%5B%5D=1&priceMax=&oppMax=',
            'property_type': 'house'
            },
            {
            'url': 'https://www.athomepartners.be/te-huur?part=residentieel&category%5B%5D=2&priceMax=&oppMax=',
            'property_type': 'apartment'
            }
        ]

        for url in start_urls:
            yield Request(url=url.get('url'),
                          callback=self.parse,
                          meta={'page': 1,
                                'response_url': url.get('url'),
                                'property_type': url.get('property_type')})

    def parse(self, response, **kwargs):
        listings = response.xpath('.//div[contains(@class,"gallcell")]/a/@href').extract()
        for url in listings:
            url = response.urljoin(url)
            yield scrapy.Request(url=url,
                                 callback=self.get_property_details,
                                 meta={'response_url': url,
                                       'property_type': response.meta.get('property_type')})

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value('external_link', response.meta.get('response_url'))
        item_loader.add_value('external_id', response.meta.get('response_url').split('/')[-2])

        title = response.xpath('//div[contains(@class,"description")]//h2/text() |//span[@class="address"]/text()').get()
        item_loader.add_value('title', title)

        item_loader.add_xpath('address', './/div[@class="detail-map spacing-bottom"]//span/text()')
        if item_loader.get_output_value('address') and len(item_loader.get_output_value('address')) > 2:
            item_loader.add_value('city', item_loader.get_output_value('address').split(' ')[-1])
            item_loader.add_value('zipcode', item_loader.get_output_value('address').split(' ')[-2])
        
        address = ""
        if not item_loader.get_collected_values("address"):
            address = response.xpath("//span[@class='address']/text()").get()
            if address:
                item_loader.add_value("address", address)
                zipcode = [x for x in address.split(" ") if x.isdigit()]
                item_loader.add_value("zipcode", zipcode[0])
                item_loader.add_value("city", address.split(zipcode[0])[1].strip())
                
        if not item_loader.get_collected_values("title"):
            item_loader.add_value("title", address)
        
        javascript = response.xpath('.//script[contains(text(), "LatLng")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            xml_selector = Selector(text=xml)
            item_loader.add_value('latitude', xml_selector.xpath('.//identifier[@name="LatLng"]/../../../arguments/number/@value').extract()[0])
            item_loader.add_value('longitude', xml_selector.xpath('.//identifier[@name="LatLng"]/../../../arguments/number/@value').extract()[1])

        item_loader.add_xpath('rent_string', './/span[@class="price"]/text()')
        
        item_loader.add_xpath('square_meters', './/div[@class="left" and contains(text(),"Bewoonbare oppervlakte")]/following-sibling::div/text()') 
        item_loader.add_xpath('bathroom_count', './/div[@class="left" and contains(text(),"Badkamers")]/following-sibling::div/text()') 

        
        description = "".join(response.xpath("//div[contains(@class,'description')]//h2/following-sibling::text() | //div[contains(@class,'description')]//text()").getall())
        if description:
            item_loader.add_value("description", re.sub("\s{2,}", " ", description.strip()))
        # else:
        #     description = "".join(response.xptah("//div[contains(@class,'description')]//div//text()").getall())
        #     if description:
        #         item_loader.add_value("description", description.strip())
        
        
        room_count = response.xpath('//div[@class="left" and contains(text(),"Slaapkamers")]/following-sibling::div/text()').get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        elif "studio" in description.lower():
            item_loader.add_value("room_count", "1")
        
        if "studio" in description.lower():
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value('property_type', response.meta.get('property_type'))
            
        item_loader.add_xpath('images', './/a[contains(@href,"whise_images")]/@href')

        deposit = re.findall(r'(?<=Waarborg: )(\d{1} maanden huur|\d{3,5})',item_loader.get_output_value('description'))
        if deposit:
            if 'maanden huur' in deposit[0]:
                rent = response.xpath("substring-before(//span[@class='price']/text(),'/')").extract_first().replace("€","").replace(".","").strip()
                deposit_mul = int(extract_number_only(deposit[0]))
                # item_loader.add_value("zipcode", item_loader.get_collected_values("address")[0].split(',')[-1].strip())
                item_loader.add_value('deposit', int(rent)*int(deposit_mul))
            else:
                item_loader.add_value('deposit', deposit[0])

        monthly_costs = re.findall(r'(?<=Maandelijkse kosten )(\d+)',item_loader.get_output_value('description'))
        if monthly_costs:
            item_loader.add_value('utilities', monthly_costs[0])
        elif "/kosten" in description:
            utilities = description.split("/kosten")[0].split("€")[-1].strip()
            item_loader.add_value("utilities", utilities)
            
        furnished = response.xpath("//div[@class='left'][contains(.,'Gemeubeld')]/following-sibling::div/text()[.!='0']").get()
        if furnished:
            item_loader.add_value("furnished", True)
            
        item_loader.add_value('landlord_phone', '+32 3 770 82 52')
        item_loader.add_value('landlord_name', 'At Home & Partners')
        item_loader.add_value('landlord_email', 'info@ahpartners.be')

        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "Athomepartners_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
