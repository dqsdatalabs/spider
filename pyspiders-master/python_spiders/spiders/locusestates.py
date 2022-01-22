# -*- coding: utf-8 -*-
# Author: Valerii Nikitiuk
import scrapy
import re
from ..loaders import ListingLoader
import dateparser

class LocusestatesSpider(scrapy.Spider):
    name = "locusestates"
    allowed_domains = ["locusestates.com"] 
    start_urls = (
        'http://www.locusestates.com/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'
    external_source="Locusestates_PySpider_united_kingdom_en"

    def start_requests(self):
        start_urls = "https://www.locusestates.com/property-to-rent"
        yield scrapy.Request( url=start_urls, callback=self.parse, dont_filter=True )

    def parse(self, response, **kwargs):
        links = response.xpath('//div[@class="results-list"]')
        for link in links: 
            url = response.urljoin(link.xpath('.//div[contains(@class, "results-image")]/a/@href').extract_first())
            address = link.xpath('.//div[@class="results-info"]/h2/text()').extract_first() 
            room_count = link.xpath('.//div[@class="results-info"]//span[@class="bedroom"]/text()').extract_first('')
            bathrooms = link.xpath('.//div[@class="results-info"]//span[@class="bathroom"]/text()').extract_first('')  
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True, meta={'address': address, 'room_count': room_count, 'bathrooms': bathrooms})
        if response.xpath('//div[contains(@class, "results-container-list")]/following-sibling::div//a[contains(text(), "Next")]/@href'):
            next_link = response.urljoin(response.xpath('//div[contains(@class, "results-container-list")]/following-sibling::div//a[contains(text(), "Next")]/@href').extract_first())
            yield scrapy.Request(url=next_link, callback=self.parse, dont_filter=True, meta={'address': address, 'room_count': room_count, 'bathrooms': bathrooms})
    
    def get_property_details(self, response):
        # parse details of the property
        item_loader = ListingLoader(response=response)

        if response.xpath("//span[contains(.,'Let Agreed')]/text()").get(): return
        if response.xpath("//p[contains(.,'Furnished')]").get(): item_loader.add_value("furnished", True)
        dontallow=response.xpath("//h1/a/span/text()").get()
        if dontallow and "let" in dontallow.lower():
            return 
        externalid=response.url
        if externalid:
            externalid=externalid.split("locut-")[-1].split("locuv-")[-1].split("/")[0]
            item_loader.add_value("external_id",externalid)


        property_type = 'apartment' 
        external_link = response.url
        title = response.xpath('//meta[@property="og:title"]/@content').extract_first()
        address = response.meta.get('address')
        city = address.split(', ')[-2]
        zipcode = address.split(', ')[-1] 
        room_count = response.meta.get('room_count')
        bathrooms = response.meta.get('bathrooms')  
        if response.text:
            lat_lon = re.search(r'new google\.maps\.LatLng\((.*?)\)', response.text)
            if lat_lon:
        
                lat = lat_lon.group(1).split(',')[0]
                lon = lat_lon.group(1).split(',')[1]  
                item_loader.add_value('latitude', str(lat))
                item_loader.add_value('longitude', str(lon))
        rent_string = response.xpath('//span[@class="priceask"]/text()').extract_first('').strip()
        availabledate="".join(response.xpath("//div[@class='details-information']/p//text()").getall())
        if availabledate:
            available=availabledate.split("Available")[-1].strip()
            if not "now" in available.lower():
                date_parsed = dateparser.parse(available)
                if date_parsed:

                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        
        item_loader.add_value('property_type', property_type)
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('title', title)
        item_loader.add_value('address', address)
        item_loader.add_xpath('description', '//div[@class="details-information"]/p//text()')
        item_loader.add_value('rent_string', rent_string)
        item_loader.add_xpath('images', '//img[@class="sp-image"]/@data-src')
        if room_count:
            item_loader.add_value('room_count', str(room_count))

        if bathrooms: 
            item_loader.add_value('bathroom_count', str(bathrooms))
        item_loader.add_value('landlord_name', 'Locus Hackney Ltd')
        item_loader.add_value('landlord_email', 'hackney@locusestates.com')
        item_loader.add_value('landlord_phone', '020 7249 2004')
        item_loader.add_value("external_source", self.external_source)

        if not item_loader.get_collected_values("city"):
            city = response.xpath("//h1/a/text()[last()]").get()
            if city: 
                item_loader.add_value("city", city.split(",")[-2].strip())
                item_loader.add_value("zipcode", city.split(",")[-1].strip())

        yield item_loader.load_item() 
