import scrapy
import dateutil.parser
from python_spiders.helper import remove_white_spaces, get_amenities, extract_location_from_address, extract_location_from_coordinates
from python_spiders.loaders import ListingLoader
class StaabSpider(scrapy.Spider):
    name = 'staab'
    execution_type = 'testing'
    country = 'germany'
    locale = 'de'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['staab.de']
    start_urls = ['https://www.staab.de/immobilien/?post_type=immomakler_object&paged=1&vermarktungsart=miete&nutzungsart=wohnen&typ=&ort=&center=&radius=25&objekt-id=&collapse=&von-qm=0.00&bis-qm=320.00&von-zimmer=0.00&bis-zimmer=12.00&von-kaltmiete=0.00&bis-kaltmiete=5100.00&von-kaufpreis=0.00&bis-kaufpreis=1950000.00']
    position = 1

    def parse(self, response):
        for url in response.css(".property-title a::attr(href)").getall():
            yield scrapy.Request(url=url, callback=self.parse_page)
        next_page = response.css(".pages-nav span a.next::attr(href)").get()
        if next_page:
            yield scrapy.Request(url=next_page, callback=self.parse)


    def parse_page(self, response):
        title           = response.css(".property-title::text").get()
        address         = remove_white_spaces(response.xpath('//div[contains(text(), "Adresse")]/following-sibling::div/text()').get())
        external_id     = response.xpath('//div[contains(text(), "Objekt ID")]/following-sibling::div/text()').get()
        property_type   = make_property_type(response.xpath('//div[contains(text(), "Objekttyp")]/following-sibling::div/text()').get())
        floor           = response.xpath('//div[contains(text(), "Etage")]/following-sibling::div/text()').get()
        square_meters   = round(float(response.xpath('//div[contains(text(), "Wohnfläche")]/following-sibling::div/text()').get().replace("m²","").replace(",",".").strip()))

        room_count      = response.xpath('//div[contains(text(), "Zimmer")]/following-sibling::div/text()').get()
        bathroom_count  = response.xpath('//div[contains(text(), "Badezimmer")]/following-sibling::div/text()').get()
        # available_date  = fix_available(response.xpath('//div[contains(text(), "Verfügbar ab")]/following-sibling::div/text()').get())
        parking         = response.xpath('//div[contains(text(), "Stellplatz")]/following-sibling::div/text()').get()
        rent            = int(response.xpath('//div[contains(text(), "Kaltmiete")]/following-sibling::div/text()').re("[0-9]+\.*[0-9]*")[0].replace(".",""))
        utilities       = int(response.xpath('//div[contains(text(), "Nebenkosten")]/following-sibling::div/text()').re("[0-9]+\.*[0-9]*")[0].replace(".",""))
        energy_label    = response.xpath('//div[contains(text(), "Energie­effizienz­klasse")]/following-sibling::div/text()').get()
        description     = remove_white_spaces(" ".join(response.css(".panel-body p::text").getall()))
        images          = response.css("#immomakler-galleria a::attr(href)").getall()
        landlord_name   = response.xpath('//div[contains(text(), "Name")]/following-sibling::div/span/text()').get()
        landlord_email  = response.xpath('//div[contains(text(), "E-Mail Direkt")]/following-sibling::div/a/text()').get()
        landlord_phone  = response.xpath('//div[contains(text(), "Tel. Durchwahl")]/following-sibling::div/a/text()').get()
        
        balcony,diswasher,washing_machine, parking, elevator = fetch_amenities(response.css(".list-group-item::text").getall())

        zipcode = address.split(" ")
        zipcode, city = zipcode[0], zipcode[1]
        
        longitude, latitude     = extract_location_from_address(address)
        zipcode, city, address  = extract_location_from_coordinates(longitude, latitude)
        
        if parking:
            parking = True
        if room_count:
            room_count = int(float(room_count.replace(",",".")))
        if bathroom_count:
            bathroom_count = int(float(bathroom_count.replace(",",".")))
        item = ListingLoader(response=response)
        item.add_value("external_source"        ,self.external_source)
        item.add_value("external_id"            ,external_id)
        item.add_value("zipcode"                ,zipcode)
        item.add_value("city"                   ,city)
        item.add_value("address"                ,address)
        item.add_value("external_link"          ,response.url)
        item.add_value("currency"               ,"EUR")
        item.add_value("images"                 ,images)
        item.add_value("title"                  ,title)
        item.add_value("rent"                   ,rent)
        item.add_value("square_meters"          ,square_meters)
        item.add_value("room_count"             ,room_count)
        item.add_value("bathroom_count"         ,bathroom_count)
        item.add_value("property_type"          ,'apartment')
        item.add_value("parking"                ,parking)
        item.add_value("description"            ,description)
        item.add_value("landlord_phone"         ,landlord_phone)
        item.add_value("landlord_name"          ,landlord_name)
        item.add_value("landlord_email"         ,landlord_email)
        item.add_value("energy_label"           ,energy_label)
        item.add_value("balcony"                ,balcony)
        item.add_value("floor"                  ,floor)
        item.add_value("elevator"               ,elevator)
        item.add_value("utilities"              ,utilities)
        item.add_value("longitude"              ,longitude)
        item.add_value("latitude"               ,latitude)



        yield item.load_item()
    




def fetch_amenities(l):
    balcony,diswasher,washing_machine, parking, elevator = '','','','',''
    for i in l:
        if 'balkon' in i.lower():
            balcony = True
        elif 'personenaufzug' in i.lower():
            elevator = True
        elif 'dishwasher' in i.lower():
            diswasher = True
        elif 'waschmaschine' in i.lower() or 'wasch' in i.lower():
            washing_machine = True
        elif 'tierhaltung NICHT erlaubt' in i.lower():
            pets_allowed = False
        elif 'parkhaus in fußnähe' in i.lower() or 'außenstellplatz' in i.lower():
            parking = True
    return balcony,diswasher,washing_machine,parking, elevator

def make_property_type(prop):
    apartments = ['wohnung', 'loft']
    houses     = ['haus']
    prop = prop.lower()
    for apartment in apartments:
        if apartment in prop:
            return 'apartment'
    for house in houses:
        if house in prop:
            return 'house'
    return prop
