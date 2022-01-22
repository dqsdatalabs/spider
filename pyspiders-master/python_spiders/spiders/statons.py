# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
import re
from ..loaders import ListingLoader

class StatonsSpider(scrapy.Spider):
    name = "statons"
    allowed_domains = ["statons.com"]
    start_urls = (
        'http://www.statons.com/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'

    def start_requests(self):
        start_urls = [
            {'url': 'https://www.statons.com/property-search/?address_keyword=&radius=&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&property_type=22&keyword=&department=residential-lettings', 'property_type': 'apartment'},
            {'url': 'https://www.statons.com/property-search/?address_keyword=&radius=&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&property_type=9&keyword=&department=residential-lettings', 'property_type': 'house'},
            {'url': 'https://www.statons.com/property-search/?address_keyword=&radius=&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&property_type=18&keyword=&department=residential-lettings', 'property_type': 'house'}
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, 
                meta={'property_type': url.get('property_type')},
                dont_filter=True
            )

    def parse(self, response, **kwargs):
        for link in response.xpath('//div[@class="work_box"][not(contains(.,"Let Agreed"))]'):
            url = response.urljoin(link.xpath('.//div[@class="details-link"]/a/@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
        
        next_link = response.xpath('//div[@class="propertyhive-pagination"]//a[text()="→"]/@href').extract_first()
        if next_link:
            yield scrapy.Request(url=next_link, callback=self.parse, dont_filter=True, meta={'property_type': response.meta.get('property_type')})

    def get_property_details(self, response):
        # parse details of the propery
        item_loader = ListingLoader(response=response)
        property_type = response.meta.get('property_type')
        external_link = response.url
        address = response.xpath('//h2/text()').extract_first()
        if address:
            item_loader.add_value('address', address)
            item_loader.add_value('city', address.split(",")[-1].strip())

        room_count = response.xpath('//li[label[.="Bedrooms - "]]/span/text()[normalize-space()]').extract_first('')
        bathrooms = response.xpath('//li[label[.="Bathrooms - "]]/span/text()[normalize-space()]').extract_first('')
        if room_count:
            item_loader.add_value('room_count', room_count.strip())
        if bathrooms: 
            item_loader.add_value('bathroom_count', bathrooms.strip())
        script_map = response.xpath("//script[contains(.,'google.maps.LatLng(') and contains(.,'initialize_property_map()')]/text()").get()
        if script_map:
            latlng = script_map.split("google.maps.LatLng(")[1].split(");")[0]
            item_loader.add_value("latitude", latlng.split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split(",")[1].strip())

        item_loader.add_value('property_type', property_type)
        item_loader.add_value('external_link', external_link)
        item_loader.add_xpath('title', "//h2/text()")
        desc = "".join(response.xpath("//div[@class='property-details']//p[@class='room']//text()[.!='VIEW LESS']").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
        rent = "".join(response.xpath("//div[@class='sale']//text()").extract())
        if rent:
            if "pw" in rent:
                rent = "".join(filter(str.isnumeric, rent.replace(",","")))
                item_loader.add_value("rent", str(int(rent)*4))
                item_loader.add_value("currency", 'GBP')
            else:
                item_loader.add_value("rent_string", rent)
        parking = response.xpath("//li[contains(.,'Garage') or contains(.,'Parking') or contains(.,'parking') ]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        images = [x for x in response.xpath("//ul[@class='slides']/li//a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)    
        floor_plan_images = [x for x in response.xpath("//div[@class='property-info']//li//a[.='FLOORPLAN']/@href").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)         
        item_loader.add_xpath('landlord_name', '//div[@class="prpty-infos"]/strong/text()')
        item_loader.add_value('landlord_email', 'hadley@statons.com')
        item_loader.add_xpath('landlord_phone', '//div[@class="prpty-infos"]//a[contains(@href,"tel")]/text()')
        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
        yield item_loader.load_item() 