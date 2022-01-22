# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
import re
from ..loaders import ListingLoader

class CityMoveSpider(scrapy.Spider):
    name = "city_move"
    allowed_domains = ["city-move.co.uk"]
    start_urls = (
        'http://www.city-move.co.uk/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'

    def start_requests(self):
        start_urls = [
            {'url': 'http://city-move.co.uk/property-search/?property-id=&location=any&status=for-rent&type=apartment&bedrooms=1&min-price=any&max-price=any', 'property_type': 'apartment'},
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, 
                meta={'property_type': url.get('property_type')},
                dont_filter=True
            )

    def parse(self, response, **kwargs):
        links = response.xpath('//article[contains(@class, "property-item")]/h4/a')
        for link in links: 
            url = response.urljoin(link.xpath('./@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
        if response.xpath('//div[@class="pagination"]/a[contains(text(), "Next")]/@href'):
            next_link = response.xpath('//div[@class="pagination"]/a[contains(text(), "Next")]/@href').extract_first()
            yield scrapy.Request(url=next_link, callback=self.parse, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
    
    def get_property_details(self, response):

        property_type = response.meta.get('property_type')
        external_link = response.url
        room_count_text = response.xpath('//span[contains(text(), "Bedroom")]/text()').extract_first('').strip()
        if room_count_text:
            room_count = re.findall(r'\d+', room_count_text)[0]     

        lat = re.search(r'\"lat\"\:\"(.*?)\"', response.text).group(1)
        lon = re.search(r'\"lang\"\:\"(.*?)\"', response.text).group(1)
        
        item_loader = ListingLoader(response=response)
        item_loader.add_value('property_type', property_type)
        item_loader.add_value('external_link', external_link)

        desc = " ".join(response.xpath('//article[contains(@class,"property-item")]//div[contains(@class, "content")]//text()').getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        else:
            desc = " ".join(response.xpath('//article[contains(@class,"property-item")]//div[contains(@class, "content")]//text()').getall())
            if desc:
                desc = re.sub('\s{2,}', ' ', desc.strip())
                item_loader.add_value("description", desc)

        rent = response.xpath('//h5[@class="price"]/span[2]/text()').extract_first('').strip()
        if rent:
            if "pw" in rent.lower():
                rent = rent.split("£")[1].split(".")[0].replace(",","")
                item_loader.add_value("rent", int(rent)*4)
            else:
                rent = rent.split("£")[1].split(".")[0].replace(",","")
                item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")
        
        furnished = response.xpath("//div[contains(@class,'features')]//li[contains(.,'Furnished')]//text()").get()
        if furnished:
            item_loader.add_value("furnished", True)

          
        bathroom_count = "".join(response.xpath('//span[contains(text(), "Bathroom")]/text()').getall())
        if bathroom_count:
            bathroom_count = bathroom_count.strip().split("\u00a0")[0]
            item_loader.add_value("bathroom_count", bathroom_count)

        title = response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title", title)

        item_loader.add_xpath('images', '//div[@id="property-detail-flexslider"]//img/@src')
        item_loader.add_value('room_count', str(room_count))
        item_loader.add_value('latitude', str(lat))
        item_loader.add_value('longitude', str(lon))
        item_loader.add_value('landlord_name', 'City Move')
        item_loader.add_value('landlord_email', 'admin@city-move.co.uk')
        item_loader.add_value('landlord_phone', '0203 110 0700')
        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
        yield item_loader.load_item()
