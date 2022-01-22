# -*- coding: utf-8 -*-
# Author: Valerii Nikitiuk
import scrapy
from scrapy import FormRequest
from scrapy.selector import Selector
import re
import json
from ..loaders import ListingLoader

class ArcherbassettSpider(scrapy.Spider):
    name = "archerbassett"
    allowed_domains = ["www.archerbassett.co.uk"]
    start_urls = (
        'http://www.www.archerbassett.co.uk/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'

    def start_requests(self):
        for page in range(1, 7):
            start_urls = 'https://www.archerbassett.co.uk/searchform.php'
            data = {
                    'criteria': '',
                    'radius': '',
                    'minbeds': '',
                    'minprice': '',
                    'maxprice': '',
                    'view': 'L',
                    'mode': 'R',
                    'page': '{}'.format(str(page)),
                    'sortby': 'N',
                    'incsold': 'true',
                    'incletagreed': 'false'
            }
            yield FormRequest(
                url=start_urls,
                callback=self.parse, 
                formdata=data,
                dont_filter=True
            )

    def parse(self, response, **kwargs):
        data = json.loads(response.text)
        body_html = data['list']
        links = Selector(text=body_html).xpath('//a[contains(text(), "View details")]')
        for link in links: 
            url = response.urljoin(link.xpath('./@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
    
    def get_property_details(self, response):
        property_type_site = response.xpath('//div[contains(@class, "property-utilties")]//img[contains(@src, "home")]/following-sibling::text()').extract_first('').strip()
        if 'house' in property_type_site.lower() or "semi detached" in property_type_site.lower() or "terraced" in property_type_site.lower() or "maisonette" in property_type_site.lower()  or "bungalow" in property_type_site.lower() or "barn conversion" in property_type_site.lower():
            property_type = 'house'
        elif 'apartment' in property_type_site.lower():
            property_type = 'apartment'
        elif 'flat' in property_type_site.lower():
            property_type = 'apartment' 
        else:
            property_type = ''
        external_link = response.url
        address = response.xpath('//h1[contains(@class, "product-title")]/text()').extract_first('').strip()
        room_count_text = response.xpath('//div[contains(@class, "property-utilties")]//img[contains(@src, "bed")]/following-sibling::text()').extract_first('').strip()
        if room_count_text:
            room_count = re.findall(r'\d+', room_count_text)[0]     
        bathrooms_text = response.xpath('//div[contains(@class, "property-utilties")]//img[contains(@src, "bath")]/following-sibling::text()').extract_first('').strip() 
        if bathrooms_text:
            bathrooms = re.findall(r'\d+', bathrooms_text)[0]
        rent_string = response.xpath('//div[@class="price"]/span/text()').extract_first('').strip()
        latitude = re.search(r'lat:\s(.*?)\,', response.text).group(1)
        longitude = re.search(r'lng:\s(.*?)\}', response.text).group(1)
        deposit = response.xpath("//li[contains(.,'DEPOSIT')]//text()[not(contains(.,'HOLDING'))]").get()
        parking = response.xpath("//li[contains(.,'Parking') or contains(.,'Garage')]//text()").get()
        item_loader = ListingLoader(response=response)
        if property_type:
            item_loader.add_value('property_type', property_type)
            item_loader.add_xpath('title', "//title/text()")
            item_loader.add_value('external_link', external_link)
            item_loader.add_value('external_id', response.url.split("id=")[1].strip())
            item_loader.add_value('address', address)
            item_loader.add_value('city', address.split(",")[-1].strip())
            item_loader.add_xpath('description', '//h2[contains(text(), "description")]/following-sibling::text()')
            item_loader.add_value('rent_string', rent_string)
            if deposit:
                deposit = deposit.split("£")[1].split(".")[0].replace(",","")
                item_loader.add_value("deposit", deposit)
            if parking:
                item_loader.add_value("parking", True)
            item_loader.add_xpath('images', '//div[@id="big"]//img/@src')
            item_loader.add_value('latitude', str(latitude))
            item_loader.add_value('longitude', str(longitude))
            item_loader.add_value('room_count', str(room_count))
            item_loader.add_value('bathroom_count', str(bathrooms))
            item_loader.add_value('landlord_name', 'Archer Bassett')
            item_loader.add_value('landlord_email', 'careers@archerbassett.co.uk')
            item_loader.add_value('landlord_phone', '024 7623 7500')
            item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))

            yield item_loader.load_item()