# -*- coding: utf-8 -*-
# Author: Valerii Nikitiuk
import scrapy
import re
from ..loaders import ListingLoader

class NorthwesthomesSpider(scrapy.Spider):
    name = "northwesthomes"
    allowed_domains = ["www.northwesthomes.co.uk"]
    start_urls = (
        'http://www.www.northwesthomes.co.uk/', 
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator = ','
    scale_separator = '.'       
    external_source = 'Northwesthomes_PySpider_united_kingdom_en'
    def start_requests(self):
        start_urls = [
            {'url': 'https://www.northwesthomes.co.uk/search/?extra_2=%211&department=%21Commercial&instruction_type=Letting&ajax_polygon=&property_type=Apartment', 'property_type': 'apartment'},
            {'url': 'https://www.northwesthomes.co.uk/search/?extra_2=%211&department=%21Commercial&instruction_type=Letting&ajax_polygon=&property_type=Flat', 'property_type': 'apartment'},
            {'url': 'https://www.northwesthomes.co.uk/search/?extra_2=%211&department=%21Commercial&instruction_type=Letting&ajax_polygon=&property_type=Detached', 'property_type': 'house'},
            {'url': 'https://www.northwesthomes.co.uk/search/?extra_2=%211&department=%21Commercial&instruction_type=Letting&ajax_polygon=&property_type=House+Share', 'property_type': 'house'},
            {'url': 'https://www.northwesthomes.co.uk/search/?extra_2=%211&department=%21Commercial&instruction_type=Letting&ajax_polygon=&property_type=Terraced', 'property_type': 'house'},
            {'url': 'https://www.northwesthomes.co.uk/search/?extra_2=%211&department=%21Commercial&instruction_type=Letting&ajax_polygon=&property_type=Studio', 'property_type': 'studio'},
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, meta={'property_type': url.get('property_type')},
                dont_filter=True
            )

    def parse(self, response, **kwargs):
        links = response.xpath('//div[@id="search-results"]//div[contains(@class, "property-grid-image-shadow")]')   
        for link in links:
            url = response.urljoin(link.xpath('./a[contains(text(), "MORE DETAILS")]/@href').extract_first())
            room_count = str(link.xpath('.//img[contains(@src, "bed")]/@alt').extract_first())
            bathrooms = str(link.xpath('.//img[contains(@src, "bath")]/@alt').extract_first())
            yield scrapy.Request(
                url=url, 
                callback=self.get_property_details, 
                meta={
                    'property_type': response.meta.get('property_type'),
                    'room_count': room_count,
                    'bathrooms': bathrooms 
                }
            )
        next_page = response.xpath("//ul[@class='pagination']/li/a[@class='next']/@href").extract_first()
        if next_page: 
            yield scrapy.Request(url=response.urljoin(next_page), callback=self.parse, meta={'property_type': response.meta.get('property_type')})
    
    def get_property_details(self, response):
        external_link = response.url

        pagetitle=response.xpath("//div[@id='page-title']/h1/text()").get()
        if pagetitle and "Sorry, We Can't Find That Page" in pagetitle:
            return 
        
        address = response.xpath('//div[@class="thumb-address"]/text()').extract_first('').strip()
        if 'lancashire' in address.lower():
            city = 'Lancashire'
            try:
                zipcode = re.search(r'Lancashire\s(.*)', address).group(1)
            except:
                zipcode = ''
        else:
            city = ''
            zipcode = ''
        rent = response.xpath('//span[@itemprop="price"]/text()').extract_first('').strip()
        if 'pw' in rent.lower():
            rent_value = re.findall(r'([\d|,|\.]+)', rent)[0].replace(',', '')
            rent_month = str(int(rent_value) * 4) + '£' 
        elif 'pcm' in rent.lower():
            rent_month = rent
        else:
            rent_month = ''
        property_type = response.meta.get('property_type')
        room_count = response.meta.get('room_count')
        bathrooms = response.meta.get('bathrooms')
        dec = ''.join(response.xpath('//div[@id="collapseExample"]//text()').extract())
        if 'park' in dec.lower():
            parking = True
        else:
            parking = ''
        if 'furnished' in dec.lower():
            furnished = True
        else:
            furnished = ''
        lat_lon = re.search(r'q=(.*?)\"', response.text)
        
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)
        
        if lat_lon:
            lat_lon = lat_lon.group(1)
            lat = lat_lon.split('%2C')[0]
            lon = lat_lon.split('%2C')[1]

            item_loader.add_value('longitude', str(lat))
            item_loader.add_value('latitude',str(lon))
            
        title = response.xpath('//meta[@property="og:title"]/@content').extract_first('').strip()  
        item_loader.add_value('property_type', property_type)
        item_loader.add_value('external_link', external_link)
        external_id = external_link
        if external_id and "details" in external_id:
            item_loader.add_value('external_id', external_link.split("property-details/")[1].split("/")[0])
        item_loader.add_value('address', address)
        item_loader.add_xpath('description', '//div[@id="collapseExample"]//text()')
        item_loader.add_value('rent_string', rent_month)
        item_loader.add_value('bathroom_count', str(bathrooms))
        item_loader.add_value('city', city)
        item_loader.add_value('zipcode', zipcode)
        item_loader.add_xpath('images', '//img[@itemprop="image"]/@src')
        if room_count != "0":
            item_loader.add_value('room_count', str(room_count))
        elif property_type == "studio":
            item_loader.add_value('room_count', "1")

        item_loader.add_value('landlord_name', 'NorthWest Homes')
        item_loader.add_value('landlord_email', 'enquiries@northwesthomes.co.uk')
        item_loader.add_value('landlord_phone', ' 01772 821313')
        if title:
            item_loader.add_value('title', title)
        if furnished:
            item_loader.add_value('furnished', True)
        
        if parking:
            item_loader.add_value('parking', True)
        
        yield item_loader.load_item()

