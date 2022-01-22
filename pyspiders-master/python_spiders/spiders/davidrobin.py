# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
import html
from ..loaders import ListingLoader
from python_spiders.helper import string_found
import re

class DavidrobinSpider(scrapy.Spider):
    name = "davidrobin"
    allowed_domains = ["www.davidrobin.be"]
    start_urls = (
        'http://www.www.davidrobin.be/',
    )
    execution_type = 'testing'
    country = 'belgium'
    locale ='nl'
    thousand_separator='.'
    scale_separator=','

    def start_requests(self):
        start_urls = [
            {'url': 'https://www.davidrobin.be/a-louer.php?radioFilter=alouer&cityFilter=&typeFilter=Appartements+&priceFilter=&bedroomFilter=', 'property_type': 'apartment'},
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, meta={'property_type': url.get('property_type')},
                dont_filter=True
            )

    def parse(self, response, **kwargs):
        next_links = response.xpath("//div[contains(@class,'item_bien')]/div/a")
        for next_link in next_links:
            
            url = response.urljoin(next_link.xpath('./@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, meta={'property_type': response.meta.get('property_type')})
   
    def get_property_urls(self, response):
        links = response.xpath('//div[@class="blc-bien"]//div[@class="item"]')
        for link in links:
            url = response.urljoin(link.xpath('.//a/@href').extract_first(''))
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
   
    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        property_type = response.meta.get('property_type')
        external_id = response.url.split("/")[-1].strip()
        adderss = response.xpath("//div[@class='content']/p/text()").extract_first()
        if adderss:
            item_loader.add_value('address', adderss)
            item_loader.add_value('city', adderss.split("-")[1].split(" ")[-1].strip())
            item_loader.add_value('zipcode', adderss.split("-")[1].strip().split(" ")[0].strip())
        
        if property_type:
            item_loader.add_value('property_type', property_type)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value('external_id', external_id)

        images = [x for x in response.xpath("//div[@class='photos']/div/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        rent_string = response.xpath("//h1/span/text()").extract_first()
        if rent_string:
            item_loader.add_value('rent_string', rent_string.replace(" ",""))
        item_loader.add_xpath('title', "//title/text()")
        item_loader.add_xpath('description', "//div[@id='description']/p/text()")
        item_loader.add_xpath('square_meters', "//div[@class='icons']//div[@class='item']//img[@alt='icone maison']//following-sibling::span/text()")
        item_loader.add_xpath('room_count', "//div[@class='icons']//div[@class='item']//img[@alt='icone lit']//following-sibling::span/text()")
        
        bathroom_count = response.xpath("//div[img[contains(@src,'bathroom')]]/span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        lat_lng = response.xpath("//iframe/@src[contains(.,'map')]").get()
        if lat_lng:
            latitude = lat_lng.split("q=")[1].split(",")[0]
            longitude = lat_lng.split("q=")[1].split(",")[1].split("&")[0]
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        item_loader.add_xpath('landlord_name', "//div[@class='rdv']/h3/text()")
        item_loader.add_xpath('landlord_email', "//div[@class='rdv']/p/a[contains(.,'@')]/text()")
        item_loader.add_xpath('landlord_phone', "//div[@class='rdv']/p/a[contains(@class,'tel')]/text()")
        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
        yield item_loader.load_item()
