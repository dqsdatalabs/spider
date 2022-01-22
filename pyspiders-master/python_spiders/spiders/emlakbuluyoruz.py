# -*- coding: utf-8 -*-
# Author: Valerii Nikitiuk
import scrapy
import re
from ..loaders import ListingLoader
from python_spiders.helper import string_found, format_date


class EmlakbuluyoruzSpider(scrapy.Spider):
    name = 'emlakbuluyoruz'
    allowed_domains = ['emlakbuluyoruz']
    start_urls = ['https://www.emlakbuluyoruz.com/']
    execution_type = 'testing'
    country = 'turkey'
    locale ='tr'
    thousand_separator='.'
    scale_separator=','
    
    def start_requests(self):
        start_urls = [
            {'url': 'https://www.emlakbuluyoruz.com/kiralik/konut/apartman-dairesi/antalya', 'property_type': 'apartment'},
            {'url': 'https://www.emlakbuluyoruz.com/kiralik/konut/villa/antalya', 'property_type': 'house'}
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, meta={'property_type': url.get('property_type')},
                dont_filter=True
            )

    def parse(self, response, **kwargs):
        
        links = response.xpath('//span[@class="divTableCell"]/a')
        for link in links: 
            
            url = response.urljoin(link.xpath('./@href').extract_first())
            sale = link.xpath('./text()[contains(.,"SATILIK")]').extract_first()
            if sale:
                continue
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True, meta={'property_type': response.meta.get('property_type')})

        if response.xpath('//a[contains(text(), "»")]/@href'):
            next_link = response.urljoin(response.xpath('//a[contains(text(), "»")]/@href').extract_first())
            yield scrapy.Request(url=next_link, callback=self.parse, dont_filter=True, meta={'property_type': response.meta.get('property_type')})

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        external_id = response.xpath('//dt[contains(text(), "İlan No")]/following-sibling::dd[@class="ilanNo"]/text()').extract_first().strip()
        external_link = response.url
        title = response.xpath('//title/text()').extract_first()
        available_date = response.xpath('//dt[contains(text(), "Tarihi")]/following-sibling::dd/text()').extract_first().strip()
        address = response.xpath('//span[@class="detay_title"]/../following-sibling::div/text()').extract_first()
        room_count_text = response.xpath('//dt[contains(text(), "Oda")]/following-sibling::dd/text()').extract_first()
        if '+' in room_count_text: 
            room_count_value = room_count_text.split('+')
            room_count = str(int(room_count_value[0]) + int(room_count_value[1]))
        elif 'null' in room_count_text:
            room_count = ''  
        else:
            room_count = room_count_text 
        try:
            lat_lon = re.search(r'var text\s=\s\'(.*?)\'', response.text).group(1)
            lat = str(lat_lon.split(',')[0])
            lon = str(lat_lon.split(',')[1])  
        except:
            lat = lon = ''

        deposit= response.xpath("//div[@class='commonProp']/dl/dt[.='Depozito']/following-sibling::dd[1]/text()[.!='-']").extract_first()
        if deposit:
            item_loader.add_value('deposit', deposit.replace(".","").strip())

        square_meters = str(response.xpath('//dt[contains(text(), "Metrekare")]/following-sibling::dd/text()').extract_first(''))
        details_text = ' '.join(response.xpath('//div[@class="propDetail"]/ul/li//text()').extract())
        
        property_type = response.meta.get('property_type')
        landlord_phone = response.xpath('//a[@id="webAra"]/text()').extract_first().strip()
        floor = response.xpath('//dt[contains(text(), "Kat")]/following-sibling::dd/text()').extract_first('').strip() 
        rent_text = ''.join(response.xpath('//span[@class="detay_title"]/..//text()').extract())
        if 'EUR' in rent_text:
            rent = rent_text + "€"
            currency = "EUR"
        else:
            rent = rent_text
            currency = "TRY"
        if rent:
            if "günlük" in rent.lower():
                rent = "".join(filter(str.isnumeric, rent.replace(',', '').replace('\xa0', '')))
                item_loader.add_value("rent", str(int(float(rent)*30)))  
                item_loader.add_value("currency", currency)  
            elif "yıllık" in rent.lower():
                rent = "".join(filter(str.isnumeric, rent.replace(',', '').replace('\xa0', '')))
                item_loader.add_value("rent", str(int(float(rent)/12)))
                item_loader.add_value("currency", currency)  
            elif "aylık" in rent.lower():
                rent = "".join(filter(str.isnumeric, rent.replace(',', '').replace('\xa0', '')))
                item_loader.add_value("rent", str(int(float(rent))))
                item_loader.add_value("currency", currency) 
        if property_type:
            item_loader.add_value('property_type', property_type)
        item_loader.add_value('external_id', external_id)
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('title', title)
        item_loader.add_value('address', address)
        item_loader.add_value('city', address.split("/")[0].strip())
        description = "".join(response.xpath("//textarea[@id='EmlakAciklama']//text()").extract())
        if description:
            cleanr = re.compile('<.*?>')
            cleantext = re.sub(cleanr, '', description)
            item_loader.add_value('description', cleantext)

        item_loader.add_xpath('bathroom_count', "//div[@class='commonProp']/dl/dt[.='Banyo Sayısı']/following-sibling::dd[1]/text()")
       
        item_loader.add_xpath('images', '//div[@id="detailSlider"]//img[@class="rsTmb"]/@src')
        item_loader.add_value('square_meters', square_meters)
        item_loader.add_value('floor', floor)

        if available_date:
            item_loader.add_value('available_date', format_date(available_date))
        if string_found(['otopark'], details_text.lower()):
            item_loader.add_value('parking', True)
        if string_found(['balkon'], details_text.lower()):
            item_loader.add_value('balcony', True)
        if string_found(['asansör'], details_text.lower()):
            item_loader.add_value('elevator', True)
        if string_found(['terrasse', 'teras'], details_text.lower()):
            item_loader.add_value('terrace', True)
        if string_found(['mobilyalı'], details_text.lower()) or string_found(['eşyalı'], details_text.lower()):
            item_loader.add_value('furnished', True)
        if lat: 
            item_loader.add_value('latitude', lat)    
        if lon: 
            item_loader.add_value('longitude', lon)
        item_loader.add_value('room_count', room_count)
        item_loader.add_value('landlord_name', 'emlakbuluyoruz')
        item_loader.add_value('landlord_email', 'destek@emlakbuluyoruz.com')
        item_loader.add_value('landlord_phone', landlord_phone)
        item_loader.add_value('external_source', 'Emlakbuluyoruz_PySpider_turkey_tr')
        yield item_loader.load_item()


         