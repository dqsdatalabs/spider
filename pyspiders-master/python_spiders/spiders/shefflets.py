# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
import re
from ..loaders import ListingLoader

def extract_city_zipcode(_address):
    if ',' in _address:
        city = _address.split(", ")[-2]
        zipcode = " ".join(_address.split(", ")[-1].strip().split(" ")[-2:])
    else:
        city = ''
        zipcode = ''
    return city, zipcode

class SheffletsSpider(scrapy.Spider):
    name = "shefflets"
    allowed_domains = ["shefflets.com"]
    start_urls = (
        'http://www.shefflets.com/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'

    def start_requests(self):
        start_urls = [
            {'url': 'https://www.shefflets.com/properties/?page=1&pageSize=12&orderBy=PriceSearchAmount&orderDirection=DESC&propInd=L&businessCategoryId=1&searchType=grid&hideProps=&lettingsClassificationRefIds=95&lettingsClassificationRefIds=94&lettingsClassificationRefIds=-1', 'property_type': 'house'}
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, 
                meta={'property_type': url.get('property_type')},
                dont_filter=True
            )

    def parse(self, response, **kwargs):
        links = response.xpath('//div[@class="photo-cropped"]/a')
        for link in links: 
            url = response.urljoin(link.xpath('./@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
        if response.xpath('//div[@class="pagerpagenumbers"]//a[contains(text(), "next")]/@href'):
            next_link = response.urljoin(response.xpath('//div[@class="pagerpagenumbers"]//a[contains(text(), "next")]/@href').extract_first())
            yield scrapy.Request(url=next_link, callback=self.parse, dont_filter=True, meta={'property_type': response.meta.get('property_type')})

    def get_property_details(self, response):
        # parse details of the property
        
        item_loader = ListingLoader(response=response)

        let_agreed = response.xpath("//img[@alt='Let Agreed']/@alt").get()
        let = response.xpath("//img[@alt='Let']/@alt").get()
        if let_agreed or let:
            return

        title = response.xpath("//title/text()").get()
        if title: item_loader.add_value('title', title.strip())

        property_type = response.meta.get('property_type')
        external_link = response.url
        external_id = response.xpath('//div[@class="reference"]/text()').extract_first('').split(': ')[-1]
        address = response.xpath('//div[@class="address"]/text()').extract_first()
        city, zipcode = extract_city_zipcode(address) 
        room_count = response.xpath('//span[@class="beds"]/text()').extract_first('').strip()
        bathrooms = response.xpath('//span[@class="bathrooms"]/text()').extract_first('').strip() 
        lat_lon_text = response.xpath('//div[@id="maplinkwrap"]/a/@href').extract_first()
        try:
            lat = re.search(r'lat=(.*?)&', lat_lon_text).group(1)
        except:
            lat = ''
        try:
            lon = re.search(r'lng=(.*?)&', lat_lon_text).group(1) 
        except:
            lon = ''
        rent_string = "".join(response.xpath('//div[@class="price"]//text()').extract())
        if rent_string:
            if "pcm" in rent_string:
                item_loader.add_value("rent_string", rent_string)
            else:
                rent_value = re.findall(r'([\d|,|\.]+)', rent_string)[0].replace(',', '')
                rent_month = str(int(float(rent_value)) * 4) + '£'
                item_loader.add_value('rent_string', rent_month)
        furnished = response.xpath("//li//text()[contains(.,'Furnished') or contains(.,'furnished')]").extract_first()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value('furnished', False)
            elif "furnished" in furnished.lower():
                item_loader.add_value('furnished', True)

        item_loader.add_value('property_type', property_type)
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('external_id', external_id)
        item_loader.add_value('address', address)
        item_loader.add_value('city', city)
        item_loader.add_value('zipcode', zipcode)
        item_loader.add_xpath('description', '//div[@class="description"]/text()')
        item_loader.add_xpath('images', '//img[@class="propertyimage"]/@src')
        if room_count:
            item_loader.add_value('room_count', str(room_count))
        if lat:
            item_loader.add_value('latitude', str(lat))
        if lon:
            item_loader.add_value('longitude', str(lon))
        if bathrooms: 
            item_loader.add_value('bathroom_count', str(bathrooms))
        item_loader.add_value('landlord_name', 'Sheff Lets')
        item_loader.add_value('landlord_email', 'info@shefflets.com')
        item_loader.add_value('landlord_phone', '0114 321 77 77')
        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
        yield item_loader.load_item() 