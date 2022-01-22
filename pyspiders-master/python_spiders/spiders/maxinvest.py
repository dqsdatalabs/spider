# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
import re
from ..loaders import ListingLoader
from python_spiders.helper import string_found

class MaxinvestSpider(scrapy.Spider):
    name = "maxinvest"
    execution_type = 'testing'
    country = 'belgium'
    locale ='nl'
    thousand_separator='.'
    scale_separator=','
    external_source="Maxinvest_PySpider_belgium_nl"

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.maxinvest.be/a-louer?view=list&page=1&goal=1&ptype=3",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://www.maxinvest.be/a-louer?view=list&page=1&goal=1&ptype=1",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "http://www.maxinvest.be/a-louer?view=list&page=1&goal=1&ptype=2",
                ],
                "property_type" : "studio"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield scrapy.Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    def parse(self, response, **kwargs):
    
        links = response.xpath('//div[@id="PropertyListRegion"]//div[contains(@class, "items")]//a')
        for link in links:
            url = response.urljoin(link.xpath('./@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True, meta={"property_type": response.meta["property_type"]})    
            
    def get_property_details(self, response): 
        title = response.xpath('//div[@id="PropertyRegion"]//h3[contains(@class, "leftside")]/text()').extract_first().strip()
        content = title.lower()
        external_id = response.url.split("&id=")[-1].strip()
        address = response.xpath('//div[contains(text(), "Adresse")]/../div[@class="value"]/text()').extract_first()
        city_zipcode = address.split(', ')[-1]
        zipcode, city = city_zipcode.split(' ')
        room_count = response.xpath('//div[contains(text(), "Chambre")]/../div[@class="value"]/text()').extract_first()
        if room_count:
            room_count=room_count.strip()
            room_count = str(re.findall(r'\d+', room_count)[0])
        item_loader = ListingLoader(response=response)

        import dateparser
        available_date = response.xpath("//div[@class='name' and contains(text(),'Disponibilité')]/following-sibling::div[@class='value']/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"], languages=['fr'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

        latitude = response.xpath("//iframe[@id='streetViewFrame']/@src").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split('ll=')[1].split(',')[0].strip())
            item_loader.add_value("longitude", latitude.split('ll=')[1].split(',')[1].split('&')[0].strip())

        item_loader.add_value('property_type', response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value('external_id', external_id)
        item_loader.add_value('address', address)
        item_loader.add_value('city', city)
        item_loader.add_value('zipcode', zipcode)
        item_loader.add_value('title', title)
        item_loader.add_xpath('description', '//div[contains(text(), "Description")]/following-sibling::div/div/text()')
        item_loader.add_xpath('floor', '//th[contains(text(), "Floors (number)")]/following-sibling::td/text()')
        item_loader.add_xpath('rent_string', '//div[contains(text(), "Prix")]/../div[@class="value"]/text()')
        item_loader.add_xpath('images', '//div[@class="swiper-wrapper"]//img/@src')
        item_loader.add_xpath('square_meters', '//div[contains(text(), "Superficie totale")]/../div[@class="value"]/text()')
        item_loader.add_value('room_count', room_count)
        item_loader.add_value('landlord_name', 'MAXInvest')
        item_loader.add_value('landlord_email', 'maxime@maxinvest.be')
        item_loader.add_value('landlord_phone', '+32 498 51 56 53')
        item_loader.add_value("external_source", self.external_source)

        bathroom_count = response.xpath("//div[contains(text(),'de bain')]/following-sibling::div[@class='value']/text()").get()
        if bathroom_count: item_loader.add_value("bathroom_count", bathroom_count.strip())

        parking = response.xpath("//div[contains(text(),'Garage')]/following-sibling::div[@class='value']/text()").get()
        if parking:
            if int(parking.strip()) > 0: item_loader.add_value("parking", True)
            else: item_loader.add_value("parking", False)

        yield item_loader.load_item()
